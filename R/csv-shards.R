# ---------------------------------------------------------------------------
# Byte-level CSV helpers for streaming (appending) shard files.
#
# A producer appends CSV records to shard files and seals each shard by
# creating a marker file. While a shard is still being written, its last
# bytes may contain:
#   * an incomplete UTF-8 multibyte character,
#   * a quoted record spanning several physical lines that is not closed,
#   * a final physical line without a line terminator that is not closed.
# None of those may be emitted as records until the shard is sealed and its
# tail can be validated as a complete, well-formed record.
# ---------------------------------------------------------------------------

# Read a file as raw bytes. A missing file gives NULL so callers can poll
# before the producer has created anything.
read_raw_bytes <- function(path) {
  if (!file.exists(path)) {
    return(NULL)
  }
  size <- file.info(path)$size
  con <- file(path, open = "rb")
  on.exit(close(con), add = TRUE)
  readBin(con, what = "raw", n = if (is.na(size)) .Machine$integer.max else size)
}

# A UTF-8 BOM may appear at the very start of the first shard.
strip_bom <- function(bytes) {
  if (length(bytes) >= 3L &&
      bytes[1] == as.raw(0xefL) &&
      bytes[2] == as.raw(0xbbL) &&
      bytes[3] == as.raw(0xbfL)) {
    return(bytes[-c(1L, 2L, 3L)])
  }
  bytes
}

# Return the number of trailing raw bytes that cannot be the start of a
# complete UTF-8 character sequence. Valid continuation/leading bytes are
# classified from their bit patterns; an invalid byte yields 0 (the whole
# sequence then fails strict UTF-8 validation elsewhere).
incomplete_utf8_tail <- function(bytes) {
  n <- length(bytes)
  if (n == 0L) return(0L)
  max_back <- min(n, 4L)
  for (i in seq_len(max_back)) {
    b <- as.integer(bytes[n - i + 1L])
    if (b < 0x80L) {
      # ASCII byte: a complete character on its own, nothing pending.
      return(0L)
    }
    if (b >= 0xc0L) {
      # Leading byte of a multibyte sequence: expected total length follows
      # from the number of leading 1 bits.
      if (b >= 0xf0L) {
        expected <- 4L
      } else if (b >= 0xe0L) {
        expected <- 3L
      } else {
        expected <- 2L
      }
      if (i == expected) return(0L)   # full multibyte char is present
      if (i < expected) return(i)     # continuation bytes are missing
      return(0L)                      # malformed: reported by strict decode
    }
    # b is a continuation byte (0x80-0xbf): keep scanning backwards.
  }
  # The whole window consists of continuation bytes, so the leading byte is
  # in an earlier part of the file; whatever is here is complete.
  0L
}

# Strict UTF-8 validation. Returns TRUE when every byte decodes round-trip.
is_valid_utf8 <- function(bytes) {
  if (length(bytes) == 0L) return(TRUE)
  out <- suppressWarnings(tryCatch(
    iconv(rawToChar(bytes), from = "UTF-8", to = "UTF-8", mark = FALSE),
    error = function(e) NA_character_
  ))
  !is.na(out)
}

csv_special_bytes <- function(sep, quote) {
  list(
    sep = charToRaw(sep),
    quote = charToRaw(quote),
    lf = as.raw(0x0aL),
    cr = as.raw(0x0dL)
  )
}

# Split raw CSV bytes into *physical* lines, honouring double quotes so that
# a line break embedded inside a quoted field does not terminate a record.
# Returns a list with:
#   lines       list of raw() vectors, each terminated by a line break (the
#               terminator is stripped; a trailing CR of CRLF is removed);
#   quote_open  logical vector, one per line: TRUE when an odd number of
#               un-doubled quote bytes has been seen up to and including
#               the line, i.e. a quoted record is still open;
#   pending     raw bytes after the final line break (no terminator);
#   pending_open whether the pending bytes currently hold an open quote.
split_csv_lines <- function(bytes, spec) {
  n <- length(bytes)
  if (n == 0L) {
    return(list(
      lines = list(), quote_open = logical(),
      pending = raw(), pending_open = FALSE
    ))
  }
  q <- spec$quote
  lf <- spec$lf
  cr <- spec$cr
  lf_pos <- which(bytes == lf)
  is_quote <- bytes == q
  # cumulative parity of quote bytes: doubling ("") flips parity twice and
  # therefore keeps a correctly escaped field closed.
  parity <- cumsum(is_quote) %% 2L
  starts <- c(1L, lf_pos + 1L)
  ends <- c(lf_pos - 1L, n)
  lines <- vector("list", length(lf_pos))
  quote_open <- logical(length(lf_pos))
  for (i in seq_along(lf_pos)) {
    a <- starts[i]
    b <- ends[i]
    line <- if (b >= a) bytes[a:b] else raw()
    # strip a CR so CRLF and LF are treated as the same terminator
    if (length(line) > 0L && line[length(line)] == cr) {
      line <- line[-length(line)]
    }
    lines[[i]] <- line
    quote_open[i] <- parity[lf_pos[i]] == 1L
  }
  # bytes beyond the last LF form a physical line with no terminator
  a <- starts[length(starts)]
  pending <- if (a <= n) bytes[a:n] else raw()
  pending_open <- if (length(pending) > 0L) parity[n] == 1L else FALSE
  list(
    lines = lines, quote_open = quote_open,
    pending = pending, pending_open = pending_open
  )
}

# Group physical lines into complete CSV records. A line emitted while a
# quote is open continues a record until a line closes it. Returns a list
# of raw() vectors, one per complete record (line breaks inside quotes are
# preserved as LF), plus any bytes that belong to a record not yet closed
# by a line terminator.
join_csv_records <- function(parts) {
  lines <- parts$lines
  open <- parts$quote_open
  # A terminated, non-quoted, completely empty physical line is treated as a
  # blank line and skipped rather than as a zero-field record.
  keep <- which(open | lengths(lines) > 0L)
  lines <- lines[keep]
  open <- open[keep]
  records <- list()
  if (length(lines) > 0L) {
    buf <- lines[[1]]
    buf_open <- open[[1]]
    for (i in seq_along(lines)[-1L]) {
      if (buf_open) {
        buf <- c(buf, as.raw(0x0aL), lines[[i]])
        buf_open <- open[[i]]
      } else {
        records <- c(records, list(buf))
        buf <- lines[[i]]
        buf_open <- open[[i]]
      }
    }
    if (!buf_open) {
      records <- c(records, list(buf))
      pending <- parts$pending
      pending_open <- parts$pending_open
    } else {
      # an unterminated quoted record among terminated lines: fold the
      # closed lines already buffered into the pending bytes for later
      pending <- c(buf, as.raw(0x0aL), parts$pending)
      pending_open <- TRUE
    }
  } else {
    pending <- parts$pending
    pending_open <- parts$pending_open
  }
  list(
    records = records, pending = pending, pending_open = pending_open
  )
}

# Quote-aware single-pass scan of one raw record (no line terminator) into
# character fields. Separators inside quotes are literal; quotes are handled
# per RFC 4180 (a quoted field opens at the first byte and doubles ("") to
# escape). Throws a "csv_shard_parse_error" on malformed quoting.
parse_csv_record <- function(record, spec) {
  if (length(record) == 0L) {
    return(character())
  }
  sep <- spec$sep
  quote <- spec$quote
  n <- length(record)
  fields <- character()
  field <- raw()
  i <- 1L
  in_quotes <- FALSE
  field_started_quoted <- FALSE
  while (i <= n) {
    b <- record[i]
    if (!in_quotes) {
      if (b == sep) {
        fields <- c(fields, rawToChar(field))
        field <- raw()
        field_started_quoted <- FALSE
        i <- i + 1L
      } else if (b == quote) {
        if (length(field) == 0L && !field_started_quoted) {
          in_quotes <- TRUE
          field_started_quoted <- TRUE
          i <- i + 1L
        } else {
          abort_csv("A bare field contains an unescaped quote.")
        }
      } else {
        field <- c(field, b)
        i <- i + 1L
      }
    } else {
      if (b == quote) {
        if (i < n && record[i + 1L] == quote) {
          field <- c(field, quote) # escaped "" -> one quote
          i <- i + 2L
        } else {
          in_quotes <- FALSE      # closing quote
          i <- i + 1L
        }
      } else {
        field <- c(field, b)
        i <- i + 1L
      }
    }
  }
  if (in_quotes) {
    abort_csv("A quoted field is not closed before the next separator.")
  }
  c(fields, rawToChar(field))
}

abort_csv <- function(message) {
  rlang::abort(message, class = "csv_shard_parse_error")
}

# Parse complete raw records into a character matrix. Every record must have
# the same number of fields.
records_to_text <- function(records, spec) {
  if (length(records) == 0L) {
    return(matrix(character(), nrow = 0L, ncol = 0L))
  }
  parsed <- lapply(records, parse_csv_record, spec = spec)
  widths <- lengths(parsed)
  if (length(unique(widths)) != 1L) {
    abort_csv(sprintf(
      "Records have inconsistent field counts (%s).",
      paste(unique(widths), collapse = ", ")
    ))
  }
  do.call(rbind, parsed)
}

# Full parse of raw bytes.
#
# `allow_pending = TRUE` (an unsealed shard): complete records are returned
# and any unfinished UTF-8 character / unclosed quoted record / unterminated
# final line is kept in `pending` and never emitted.
#
# `allow_pending = FALSE` (a sealed shard): any pending bytes, malformed
# quoting or invalid UTF-8 are hard errors, so a sealed shard is only
# accepted when every byte forms a complete, valid record.
parse_csv_bytes <- function(bytes, spec, allow_pending) {
  bytes <- strip_bom(bytes)
  # An unfinished multibyte character at the tail is detached before strict
  # validation: in pending mode the bytes are simply held, while a sealed
  # shard must not end this way at all.
  n_pending_utf8 <- incomplete_utf8_tail(bytes)
  if (n_pending_utf8 > 0L && !allow_pending) {
    abort_csv("The sealed shard ends on an incomplete UTF-8 character.")
  }
  if (n_pending_utf8 > 0L) {
    cut <- length(bytes) - n_pending_utf8
    held <- bytes[(cut + 1L):length(bytes)]
    bytes <- if (cut > 0L) bytes[seq_len(cut)] else raw()
  } else {
    held <- raw()
  }
  if (!is_valid_utf8(bytes)) {
    abort_csv("The shard is not valid UTF-8.")
  }
  parts <- split_csv_lines(bytes, spec)
  joined <- join_csv_records(parts)
  pending <- c(joined$pending, held)
  if (!allow_pending && length(pending) > 0L) {
    abort_csv(paste(
      "The sealed shard has bytes that do not form a terminated record:",
      "an unclosed quote or a final line without a line ending."
    ))
  }
  text <- records_to_text(joined$records, spec)
  list(
    text = text,
    pending = pending,
    pending_open = isTRUE(joined$pending_open),
    n_records = NROW(text)
  )
}
