# ---------------------------------------------------------------------------
# Pollable, persistent tsibble source backed by CSV shards that a producer
# appends to and seals in an explicit order.
#
# Sealing protocol
# ----------------
# Shard files match `pattern` (default "part-([0-9]+)\\.csv") and are read
# in the numeric order of the capture group. A shard is considered sealed
# only when its marker file exists (the shard path plus `marker_suffix`,
# default ".done"). A final `end_marker` file signals that the producer has
# stopped. The tail of an unsealed shard is never treated as the end of
# collection.
#
# Publication rules
# -----------------
# Only complete, valid records ever reach the published tsibble: no
# incomplete UTF-8 characters, no records spanning unterminated quoted
# lines and no unterminated final line. Sealed shards additionally go
# through strict tail validation and fixed header/type checks. A
# cumulative key/index duplicate or any parse failure keeps the previously
# published data untouched; repeated polls never double-count records.
# ---------------------------------------------------------------------------

#' Pollable persistent CSV-shard source for a tsibble
#'
#' `r lifecycle::badge('experimental')`
#'
#' A `csv_tsibble_source` watches a directory in which a producer writes
#' comma-separated shard files and seals them, in order, by dropping marker
#' files. Repeated calls to [poll()] incorporate every shard sealed since
#' the previous poll while never emitting partial records from the shard
#' that is still being appended to.
#'
#' @param path Directory containing the shards and marker files.
#' @param index <[`tidy-select`][dplyr_tidy_select]> The time-index column.
#' @param key <[`tidy-select`][dplyr_tidy_select]> Key column(s), `NULL` for
#'   an empty key.
#' @param pattern A regular expression matching shard file names. It must
#'   contain exactly one capture group giving the shard sequence number.
#' @param marker_suffix Suffix appended to a sealed shard's file name to
#'   form the marker name; the marker's mere existence denotes sealing.
#' @param end_marker File name (inside `path`) whose presence means the
#'   producer has finished writing.
#' @param sep,quote Field separator and quote character (single bytes).
#' @param col_types An optional named list of coercion functions applied to
#'   the matching columns after the fixed header is checked. When `NULL`,
#'   column types are inferred from the first sealed data shard and then
#'   held fixed.
#' @param regular Passed to [build_tsibble()]; `TRUE` derives a regular
#'   interval, `FALSE` builds an irregular tsibble.
#'
#' @details
#' A shard is parsed only after its marker exists. Sealed shards are
#' validated strictly: valid UTF-8 throughout, every byte consumed into a
#' terminated record, a header identical to the established fixed header,
#' and column types matching the established types. The currently open
#' shard is scanned defensively so that complete records can be inspected
#' via [pending()], but nothing from it is published until it is sealed.
#'
#' If incorporating newly sealed shards would produce duplicate key/index
#' pairs or fail validation, the error is raised (or, with
#' `poll(..., strict = FALSE)`, warned about) and the source keeps serving
#' the last published tsibble. The interval is rebuilt from scratch on
#' every successful publication, so it always reflects the complete data
#' rather than a stale estimate; when it cannot be determined (for example
#' a single observation per key) it is reported as unknown via `"?"`.
#'
#' @return An environment of class `csv_tsibble_source`.
#' @export
#'
#' @examples
#' \dontrun{
#' src <- csv_tsibble_source(tempdir(), index = time, key = sensor)
#' poll(src)
#' snapshot(src)
#' is_complete(src)
#' }
csv_tsibble_source <- function(path, index, key = NULL,
                               pattern = "part-([0-9]+)[.]csv$",
                               marker_suffix = ".done",
                               end_marker = "_END",
                               sep = ",", quote = "\"",
                               col_types = NULL,
                               regular = TRUE) {
  if (!is_string(path) || !dir.exists(path)) {
    rlang::abort("`path` must be an existing directory.")
  }
  n_groups <- tryCatch(
    length(unlist(gregexpr("\\((?!\\?)", pattern, perl = TRUE))),
    error = function(e) -1L
  )
  if (!is_string(pattern) || n_groups != 1L) {
    rlang::abort(
      "`pattern` must be a regular expression with exactly one capture group."
    )
  }
  if (!is_string(sep) || nchar(sep, type = "bytes") != 1L ||
      !is_string(quote) || nchar(quote, type = "bytes") != 1L) {
    rlang::abort("`sep` and `quote` must each be a single byte.")
  }
  if (!is_null(col_types) &&
      (!is_list(col_types) || is_empty(names(col_types)) ||
       any(names(col_types) == ""))) {
    rlang::abort("`col_types` must be a fully named list of functions or NULL.")
  }
  stopifnot(is_logical(regular, n = 1))

  env <- new.env(parent = emptyenv())
  env$path <- normalizePath(path, winslash = "/", mustWork = TRUE)
  env$index_quo <- enquo(index)
  env$key_quo <- enquo(key)
  env$pattern <- pattern
  env$marker_suffix <- marker_suffix
  env$end_marker <- end_marker
  env$spec <- csv_special_bytes(sep, quote)
  env$col_types <- col_types
  env$regular <- regular
  # runtime state
  env$consumed <- character()          # sealed shard file names incorporated
  env$cache <- list()                  # path -> list(mtime, size, parsed)
  env$header <- NULL                   # fixed header, character vector
  env$ptype <- NULL                    # zero-row typed template tibble
  env$parts <- list()                  # typed tibble per consumed shard
  env$published <- NULL                # last successfully built tbl_ts
  env$has_published <- FALSE
  env$last_error <- NULL
  env$active <- list(
    name = NA_character_, pending = character(0L),
    n_pending_bytes = 0L, quote_open = FALSE,
    incomplete_utf8 = FALSE, n_complete = 0L
  )
  structure(env, class = "csv_tsibble_source")
}

# --- shard discovery --------------------------------------------------------

# List all shard files with their numeric sequence; returns a data frame
# sorted by sequence.
discover_shards <- function(src) {
  files <- list.files(src$path, pattern = src$pattern, full.names = FALSE)
  if (length(files) == 0L) {
    return(data.frame(
      name = character(), seq = integer(),
      sealed = logical(), stringsAsFactors = FALSE
    ))
  }
  m <- regmatches(files, regexec(src$pattern, files))
  seqs <- suppressWarnings(as.integer(vapply(m, `[[`, character(1), 2L)))
  if (anyNA(seqs)) {
    bad <- files[is.na(seqs)]
    rlang::abort(sprintf(
      "Shard sequence numbers must be integers; check file(s): %s.",
      paste(bad, collapse = ", ")
    ))
  }
  if (anyDuplicated(seqs) > 0L) {
    dup <- unique(seqs[duplicated(seqs)])
    rlang::abort(sprintf(
      "Shard sequence numbers must be unique; duplicated sequence: %s.",
      paste(dup, collapse = ", ")
    ))
  }
  ord <- order(seqs, files)
  out <- data.frame(
    name = files[ord], seq = seqs[ord],
    stringsAsFactors = FALSE
  )
  out$sealed <- file.exists(file.path(src$path, paste0(out$name, src$marker_suffix)))
  out
}

# --- sealed-shard parsing & validation --------------------------------------

# Parse (or fetch from the fingerprint cache) a sealed shard into a character
# matrix and run the strict sealed-file checks. The cache key is a content
# fingerprint (size + mtime + a checksum) so a shard rewritten within the same
# filesystem timestamp granularity is always re-read rather than served stale.
parse_sealed_shard <- function(src, name) {
  path <- file.path(src$path, name)
  info <- file.info(path)
  bytes <- read_raw_bytes(path)
  if (is_null(bytes) || length(bytes) == 0L) {
    abort_shard(name, "The sealed shard is empty (a header row is required).")
  }
  fingerprint <- list(
    size = info$size, mtime = info$mtime,
    digest = unclass(tools::md5sum(path))
  )
  cache <- src$cache[[path]]
  if (!is_null(cache) && identical(cache$fingerprint, fingerprint)) {
    return(cache$parsed)
  }
  parse_error <- NULL
  parsed <- tryCatch(
    parse_csv_bytes(bytes, src$spec, allow_pending = FALSE),
    csv_shard_parse_error = function(e) {
      parse_error <<- e
      NULL
    }
  )
  if (!is_null(parse_error)) {
    abort_shard(name, conditionMessage(parse_error))
  }
  if (parsed$n_records < 1L) {
    abort_shard(name, "The sealed shard contains no header row.")
  }
  src$cache[[path]] <- list(fingerprint = fingerprint, parsed = parsed)
  parsed
}

check_header <- function(src, name, header) {
  if (is_null(src$header)) {
    if (anyDuplicated(header) > 0L) {
      abort_shard(name, sprintf(
        "The header must have unique column names; got %s.",
        paste(header, collapse = ", ")
      ))
    }
    if (length(header) < 2L) {
      abort_shard(name, "The header must contain at least a key and an index.")
    }
    src$header <- header
    return(invisible())
  }
  if (!identical(as.character(header), as.character(src$header))) {
    abort_shard(name, sprintf(
      "Fixed header mismatch.\nExpected: %s\nFound:    %s",
      paste(src$header, collapse = ", "), paste(header, collapse = ", ")
    ))
  }
}

# Coerce the data rows of one shard against the fixed template.
coerce_shard <- function(src, name, body) {
  mat <- body
  raw <- tibble::as_tibble(mat, .name_repair = "minimal")
  names(raw) <- src$header
  if (is_null(src$ptype)) {
    tbl <- coerce_columns(raw, src$col_types, name)
    check_no_parsed_na(raw, tbl, name)
    src$ptype <- tibble::as_tibble(lapply(tbl, vctrs::vec_ptype))
    return(tbl)
  }
  tbl <- coerce_columns(raw, src$col_types, name)
  check_no_parsed_na(raw, tbl, name)
  # fixed types: every later shard must be castable to the first shard's
  # ptype without a silent class downgrade (e.g. integer -> character when
  # one field is non-numeric), which vec_cast() alone would allow.
  check_fixed_classes(tbl, src$ptype, name)
  cast_error <- NULL
  tryCatch(
    tbl <- vctrs::vec_cast(tbl, src$ptype),
    error = function(e) cast_error <<- e
  )
  if (!is_null(cast_error)) {
    abort_shard(name, paste0(
      "Column types are not consistent with the first sealed shard: ",
      conditionMessage(cast_error)
    ))
  }
  tbl
}

# Columns named in `col_types` use the supplied parser; every other column
# falls back to automatic type inference so ordinary numeric/logical fields
# are not left as character merely because another column was custom-typed.
coerce_columns <- function(raw, col_types, name) {
  if (is_null(col_types)) {
    return(infer_types(raw))
  }
  unknown <- setdiff(names(col_types), names(raw))
  if (length(unknown) > 0L) {
    abort_shard(name, sprintf(
      "`col_types` names not in the header: %s.", paste(unknown, collapse = ", ")
    ))
  }
  out <- infer_types(raw)
  for (nm in names(col_types)) {
    fn <- col_types[[nm]]
    parse_error <- NULL
    value <- tryCatch(fn(raw[[nm]]), error = function(e) {
      parse_error <<- e
      raw[[nm]]
    })
    if (!is_null(parse_error)) {
      abort_shard(name, sprintf(
        "Failed to parse column `%s`: %s", nm, conditionMessage(parse_error)
      ))
    }
    out[[nm]] <- value
  }
  out
}

# Each column's storage class must already match the established ptype before
# casting, so a shard cannot widen/change a column type silently.
check_fixed_classes <- function(tbl, ptype, name) {
  for (nm in names(ptype)) {
    target <- class(ptype[[nm]])
    current <- class(tbl[[nm]])
    if (!identical(current, target)) {
      abort_shard(name, sprintf(
        "Column `%s` has type %s but the fixed type from the first sealed shard is %s.",
        nm, paste(current, collapse = "/"), paste(target, collapse = "/")
      ))
    }
  }
}

# A non-empty, non-"NA" source field that becomes an NA after coercion is a
# parse failure (e.g. "hello" cast to integer), not a legitimate missing
# value. Empty fields are allowed to parse as NA.
check_no_parsed_na <- function(raw, parsed, name) {
  for (nm in names(parsed)) {
    new_na <- is.na(parsed[[nm]]) &
      !is.na(raw[[nm]]) & nzchar(raw[[nm]]) & raw[[nm]] != "NA"
    if (any(new_na)) {
      abort_shard(name, sprintf(
        "Column `%s` has values that cannot be parsed as the fixed type: %s.",
        nm, paste(unique(raw[[nm]][new_na]), collapse = ", ")
      ))
    }
  }
}

infer_types <- function(tbl) {
  # base R's type.convert handles logical/integer/double/character; anything
  # richer (dates, ...) is supplied via `col_types`.
  converted <- lapply(tbl, utils::type.convert, as.is = TRUE,
                      numerals = "no.loss")
  tibble::as_tibble(as.data.frame(converted))
}

abort_shard <- function(name, message) {
  rlang::abort(
    sprintf("Shard `%s` failed validation: %s", name, message),
    class = "csv_tsibble_shard_error",
    shard = name
  )
}

# --- unsealed (active) shard scan -------------------------------------------

scan_active <- function(src, name) {
  reset <- list(
    name = NA_character_, pending = character(0L),
    n_pending_bytes = 0L, quote_open = FALSE,
    incomplete_utf8 = FALSE, n_complete = 0L
  )
  if (is.na(name)) {
    src$active <- reset
    return(invisible())
  }
  path <- file.path(src$path, name)
  bytes <- read_raw_bytes(path)
  if (is_null(bytes)) {
    src$active <- reset
    return(invisible())
  }
  parsed <- tryCatch(
    parse_csv_bytes(bytes, src$spec, allow_pending = TRUE),
    csv_shard_parse_error = function(e) {
      # malformed complete records in the open shard: nothing to preview;
      # the strict sealed parse will reject it on a later poll.
      list(text = matrix(character(), 0L, 0L), pending = raw(),
           pending_open = FALSE, n_records = 0L, parse_error = conditionMessage(e))
    }
  )
  pending_txt <- tryCatch(
    if (length(parsed$pending) > 0L) rawToChar(parsed$pending) else "",
    error = function(e) ""
  )
  src$active <- list(
    name = name,
    pending = pending_txt,
    n_pending_bytes = length(parsed$pending),
    quote_open = isTRUE(parsed$pending_open),
    incomplete_utf8 = incomplete_utf8_tail(bytes) > 0L,
    n_complete = parsed$n_records,
    parse_error = parsed$parse_error
  )
}

# --- publication ------------------------------------------------------------

rebuild_published <- function(src) {
  if (length(src$parts) == 0L) {
    return(invisible())
  }
  combine_error <- NULL
  data <- tryCatch(
    vctrs::vec_rbind(!!!src$parts),
    error = function(e) {
      combine_error <<- e
      NULL
    }
  )
  if (!is_null(combine_error)) {
    abort_shard(
      utils::tail(src$consumed, 1L),
      paste0("Failed to combine shards: ", conditionMessage(combine_error))
    )
  }
  build_error <- NULL
  tsbl <- tryCatch(
    build_tsibble(
      data,
      key = !!src$key_quo, index = !!src$index_quo,
      interval = src$regular, validate = TRUE
    ),
    error = function(e) {
      build_error <<- e
      NULL
    }
  )
  if (!is_null(build_error)) {
    rlang::abort(
      paste0(
        "The accumulated records do not form a valid tsibble: ",
        conditionMessage(build_error)
      ),
      class = "csv_tsibble_build_error"
    )
  }
  # publication replaces the snapshot atomically; earlier returned objects
  # are independent and therefore never mutated.
  src$published <- tsbl
  src$has_published <- TRUE
  invisible()
}

#' Poll a CSV-shard tsibble source
#'
#' Incorporates every shard sealed since the previous poll, in shard
#' sequence order, and returns the current published tsibble.
#'
#' @param x A `csv_tsibble_source`.
#' @param strict When `TRUE` (default), validation/duplicate failures raise
#'   an error while preserving the previous publication. When `FALSE`, the
#'   same condition raises a warning instead.
#' @param ... Ignored.
#'
#' @return The currently published [tbl_ts], or `NULL` if no valid data has
#'   been published yet. Repeated polls without new sealed shards return the
#'   identical object and never re-accumulate records.
#' @export
poll <- function(x, ...) {
  UseMethod("poll")
}

#' @rdname poll
#' @export
poll.csv_tsibble_source <- function(x, strict = TRUE, ...) {
  shards <- discover_shards(x)
  # the active (first not-yet-sealed) shard is previewed, never consumed
  first_open <- shards$name[!shards$sealed][1L]
  scan_active(x, first_open)

  sealed <- shards[shards$sealed, , drop = FALSE]
  # Sealing order is anchored on the highest consumed sequence. Newly sealed
  # shards must follow it without a gap, so part-(n+1) cannot be read while
  # part-n is still open; a not-yet-consumed shard may be rewritten in place.
  consumed_seqs <- shards$seq[shards$name %in% x$consumed]
  anchor <- if (length(consumed_seqs)) max(consumed_seqs) else 0L
  if (NROW(sealed) > 0L) {
    candidate_seqs <- sort(sealed$seq[sealed$seq > anchor])
    if (length(candidate_seqs) &&
        !identical(candidate_seqs,
                   seq.int(anchor + 1L, length.out = length(candidate_seqs)))) {
      msg <- "Shards must be sealed in sequence order; a sealed shard is missing an earlier predecessor."
      x$last_error <- msg
      hold_publication(x, msg, strict)
      return(x$published)
    }
  }
  new_shards <- sealed[!(sealed$name %in% x$consumed), , drop = FALSE]
  if (NROW(new_shards) == 0L) {
    x$last_error <- NULL
    return(x$published) # idempotent: no re-reading, no double accumulation
  }

  # transaction boundary: a failure at any stage restores the exact state
  # from before this poll, so the previous publication stays untouched
  state_before <- list(
    header = x$header, ptype = x$ptype, parts = x$parts,
    cache = x$cache, consumed = x$consumed,
    published = x$published, has_published = x$has_published
  )
  poll_error <- NULL
  tryCatch({
    for (i in seq_len(NROW(new_shards))) {
      name <- new_shards$name[i]
      parsed <- parse_sealed_shard(x, name)
      header <- parsed$text[1L, ]
      check_header(x, name, header)
      body <- if (parsed$n_records >= 2L) {
        parsed$text[-1L, , drop = FALSE]
      } else {
        matrix(character(), nrow = 0L, ncol = length(header))
      }
      tbl <- coerce_shard(x, name, body)
      x$parts <- c(x$parts, list(tbl))
    }
    # duplicate check across the *cumulative* data before building
    combined <- vctrs::vec_rbind(!!!x$parts)
    key_chr <- names(eval_select(x$key_quo, data = combined))
    idx_chr <- validate_index(combined, !!x$index_quo)
    if (duplicated_key_index(combined, key = key_chr, index = idx_chr)) {
      rlang::abort(
        "Cumulative key/index duplicates detected; the previously published data is retained.",
        class = "csv_tsibble_duplicate_error"
      )
    }
    # rebuild the tsibble (interval included) from the complete current data
    rebuild_published(x)
    x$consumed <- c(x$consumed, new_shards$name)
    x$last_error <- NULL
  }, error = function(e) {
    # roll back only; signalling is done outside tryCatch so the abort is
    # not immediately re-caught by this same handler.
    poll_error <<- e
    x$header <- state_before$header
    x$ptype <- state_before$ptype
    x$parts <- state_before$parts
    x$cache <- state_before$cache
    x$consumed <- state_before$consumed
    x$published <- state_before$published
    x$has_published <- state_before$has_published
    x$last_error <- conditionMessage(e)
  })
  if (!is_null(poll_error)) {
    hold_publication(x, conditionMessage(poll_error), strict)
    return(x$published) # reached in non-strict (warning) mode
  }
  x$published
}

hold_publication <- function(src, message, strict) {
  if (strict) {
    rlang::abort(message, class = "csv_tsibble_poll_error")
  } else {
    rlang::warn(message, class = "csv_tsibble_poll_warning")
  }
}

#' Current published tsibble of a CSV-shard source
#'
#' @param x A `csv_tsibble_source`.
#' @param ... Ignored.
#' @return The last published [tbl_ts], or `NULL` before the first
#'   successful publication.
#' @export
snapshot <- function(x, ...) {
  UseMethod("snapshot")
}

#' @rdname snapshot
#' @export
snapshot.csv_tsibble_source <- function(x, ...) {
  x$published
}

#' Unsealed shard preview
#'
#' Inspect the bytes of the shard currently being appended to without
#' publishing them.
#'
#' @param x A `csv_tsibble_source`.
#' @param ... Ignored.
#' @return A list with the active shard name, the held (unpublished) tail as
#'   a raw string, its byte length, whether a quote is open, whether the
#'   file ends mid UTF-8 character, and the number of complete records
#'   currently readable.
#' @export
pending <- function(x, ...) {
  UseMethod("pending")
}

#' @rdname pending
#' @export
pending.csv_tsibble_source <- function(x, ...) {
  x$active
}

#' Whether a CSV-shard source has finished
#'
#' Completion requires both the producer's end marker and a sealed marker
#' for every shard.
#'
#' @param x A `csv_tsibble_source`.
#' @param ... Ignored.
#' @return A single logical value.
#' @export
is_complete <- function(x, ...) {
  UseMethod("is_complete")
}

#' @rdname is_complete
#' @export
is_complete.csv_tsibble_source <- function(x, ...) {
  end_exists <- file.exists(file.path(x$path, x$end_marker))
  if (!end_exists) return(FALSE)
  shards <- discover_shards(x)
  NROW(shards) > 0L && all(shards$sealed)
}

#' Last poll error, if any
#'
#' @param x A `csv_tsibble_source`.
#' @param ... Ignored.
#' @return A character string describing the most recent failure, or `NULL`.
#' @export
last_poll_error <- function(x, ...) {
  UseMethod("last_poll_error")
}

#' @rdname last_poll_error
#' @export
last_poll_error.csv_tsibble_source <- function(x, ...) {
  x$last_error
}

#' @export
print.csv_tsibble_source <- function(x, ...) {
  cat("<csv_tsibble_source>\n")
  cat("  path:        ", x$path, "\n")
  cat("  sealed used: ", length(x$consumed), " shard(s)\n")
  cat("  published:   ",
      if (x$has_published) paste0(NROW(x$published), " rows") else "none yet",
      "\n")
  cat("  complete:    ", is_complete(x), "\n")
  if (!is.na(x$active$name)) {
    cat("  open shard:  ", x$active$name,
        sprintf("(%d complete record(s), %d held byte(s))\n",
                x$active$n_complete, x$active$n_pending_bytes))
  }
  invisible(x)
}

#' Generate future data from a CSV-shard source's current tsibble
#'
#' Equivalent to calling [new_data()] on the current published tsibble, so
#' future rows always derive from the data actually published by the latest
#' [poll()], and the interval used is the one rebuilt from those complete
#' records.
#'
#' @param object A `csv_tsibble_source`.
#' @param n Number of future key/index pairs per key.
#' @param ... Passed to [new_data()].
#' @return A [tbl_ts] of future rows. Errors when no data has been
#'   published or the interval is unknown/irregular.
#' @export
generate_future <- function(object, n = 1L, ...) {
  UseMethod("generate_future")
}

#' @rdname generate_future
#' @export
generate_future.csv_tsibble_source <- function(object, n = 1L, ...) {
  if (!object$has_published) {
    rlang::abort(
      "No complete records have been published yet; future data cannot be generated.",
      class = "csv_tsibble_no_data_error"
    )
  }
  # explicit, honest interval reporting rather than silently reusing an old one
  int <- interval(object$published)
  if (unknown_interval(int)) {
    rlang::abort(
      "The interval is unknown (?) from the current records; future data cannot be generated.",
      class = "csv_tsibble_unknown_interval_error"
    )
  }
  new_data(object$published, n = n, ...)
}
