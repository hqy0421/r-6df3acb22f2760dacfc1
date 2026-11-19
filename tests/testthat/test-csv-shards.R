# Tests for the pollable, persistent CSV-shard tsibble source. These cover the
# byte-level CSV guarantees (no partial UTF-8 / quoted / unterminated tail is
# ever published), the sealing protocol, fixed header/type validation,
# cumulative duplicate handling, idempotent polling and completion rules.

spec <- function() csv_special_bytes(",", "\"")

write_shard <- function(dir, name, text) {
  writeBin(charToRaw(text), file.path(dir, name))
}
seal <- function(dir, name) {
  file.create(file.path(dir, paste0(name, ".done")))
}

# ---------------------------------------------------------------------------
# Byte-level parser
# ---------------------------------------------------------------------------

test_that("CRLF and LF terminators are equivalent and BOM is stripped", {
  p <- parse_csv_bytes(charToRaw("a,b\r\n1,2\r\n3,4\r\n"), spec(), FALSE)
  expect_equal(p$n_records, 3L)
  expect_equal(p$text[2, 1], "1")
  expect_length(p$pending, 0L)

  bom <- c(as.raw(c(0xefL, 0xbbL, 0xbfL)), charToRaw("a,b\n1,2\n"))
  p <- parse_csv_bytes(bom, spec(), FALSE)
  expect_equal(p$text[1, 1], "a")
  expect_equal(p$n_records, 2L)
})

test_that("RFC 4180 quoting: doubled quotes and embedded separators", {
  p <- parse_csv_bytes(charToRaw('a,b\n1,"he said ""hi"""\n'), spec(), FALSE)
  expect_equal(p$text[2, 2], 'he said "hi"')

  p <- parse_csv_bytes(charToRaw('a,b\n1,"x,y"\n'), spec(), FALSE)
  expect_equal(ncol(p$text), 2L)
  expect_equal(p$text[2, 2], "x,y")
})

test_that("malformed quoting is rejected", {
  expect_error(
    parse_csv_bytes(charToRaw('a,b\n1,ba"d\n'), spec(), FALSE),
    "quote"
  )
  expect_error(
    parse_csv_bytes(charToRaw('a,b\n1,"unclosed,2\n'), spec(), FALSE),
    "terminated record|not closed"
  )
})

test_that("ragged field counts are rejected", {
  expect_error(
    parse_csv_bytes(charToRaw("a,b\n1,2\n3,4,5\n"), spec(), FALSE),
    "inconsistent field"
  )
})

test_that("blank terminated lines are skipped", {
  p <- parse_csv_bytes(charToRaw("a,b\n1,2\n\n3,4\n"), spec(), FALSE)
  expect_equal(p$n_records, 3L)
})

test_that("a sealed shard cannot end on an unterminated line", {
  expect_error(
    parse_csv_bytes(charToRaw("a,b\n1,2"), spec(), FALSE),
    "terminated record"
  )
  # pending mode keeps the tail instead of erroring
  p <- parse_csv_bytes(charToRaw("a,b\n1,2\n3,4"), spec(), TRUE)
  expect_equal(p$n_records, 2L)
  expect_gt(length(p$pending), 0L)
  expect_equal(rawToChar(p$pending), "3,4")
})

test_that("incomplete multibyte UTF-8 tails are detected and held", {
  expect_equal(incomplete_utf8_tail(charToRaw("中")), 0L)
  b3 <- c(charToRaw("a\nx"), as.raw(0xe4L), as.raw(0xb8L))
  expect_equal(incomplete_utf8_tail(b3), 2L)
  p <- parse_csv_bytes(b3, spec(), TRUE)
  expect_identical(p$pending, b3[3:5])

  # a sealed shard must never end mid-character
  expect_error(parse_csv_bytes(b3, spec(), FALSE), "incomplete UTF-8")
})

test_that("invalid UTF-8 anywhere is rejected even in pending mode", {
  bad <- c(charToRaw("a,b\n1,"), as.raw(0xffL), charToRaw("\n2,3\n"))
  expect_error(parse_csv_bytes(bad, spec(), TRUE), "not valid UTF-8")
})

# ---------------------------------------------------------------------------
# Source lifecycle
# ---------------------------------------------------------------------------

test_that("an empty directory publishes nothing and is not complete", {
  dir <- tempfile("csv-src-"); dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))
  src <- csv_tsibble_source(dir, index = t)
  expect_null(poll(src))
  expect_null(snapshot(src))
  expect_false(is_complete(src))
})

test_that("unsealed shard tails are never published", {
  dir <- tempfile("csv-src-"); dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))
  src <- csv_tsibble_source(dir, index = t, col_types = list(t = as.Date))

  # terminated records, still open
  write_shard(dir, "part-1.csv", "t,v\n2020-01-01,1\n2020-01-02,2\n")
  expect_null(poll(src))
  expect_equal(pending(src)$n_complete, 3L) # header counts as a complete line

  # an unterminated final line is held
  write_shard(dir, "part-1.csv",
              "t,v\n2020-01-01,1\n2020-01-02,2\n2020-01-03,3")
  poll(src)
  ap <- pending(src)
  expect_gt(ap$n_pending_bytes, 0L)
  expect_false(ap$quote_open)
  expect_null(snapshot(src))
})

test_that("a cross-line quoted record is held until sealed", {
  dir <- tempfile("csv-src-"); dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))
  src <- csv_tsibble_source(dir, index = t, col_types = list(t = as.Date))
  write_shard(dir, "part-1.csv",
              't,note\n2020-01-01,a\n2020-01-02,"line1\nline2')
  poll(src)
  expect_true(pending(src)$quote_open)
  expect_null(snapshot(src))

  write_shard(dir, "part-1.csv",
              't,note\n2020-01-01,a\n2020-01-02,"line1\nline2"\n')
  seal(dir, "part-1.csv")
  tsbl <- poll(src)
  expect_equal(NROW(tsbl), 2L)
  expect_match(tsbl$note[2], "\n", fixed = TRUE)
})

test_that("sealing publishes and rebuilds the interval from complete data", {
  dir <- tempfile("csv-src-"); dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))
  src <- csv_tsibble_source(dir, index = t, col_types = list(t = as.Date))
  write_shard(dir, "part-1.csv",
              "t,v\n2020-01-01,1\n2020-01-02,2\n2020-01-03,3\n")
  seal(dir, "part-1.csv")
  tsbl <- poll(src)
  expect_equal(NROW(tsbl), 3L)
  expect_equal(format(tsibble::interval(tsbl)), "1D")
  expect_s3_class(tsbl, "tbl_ts")
})

test_that("repeated polls are idempotent and never double-accumulate", {
  dir <- tempfile("csv-src-"); dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))
  src <- csv_tsibble_source(dir, index = t, col_types = list(t = as.Date))
  write_shard(dir, "part-1.csv", "t,v\n2020-01-01,1\n2020-01-02,2\n")
  seal(dir, "part-1.csv")
  first <- poll(src)
  expect_identical(poll(src), first)
  expect_identical(poll(src), first)
  expect_equal(NROW(snapshot(src)), 2L)
})

test_that("a rejected sealed shard keeps the previous publication", {
  dir <- tempfile("csv-src-"); dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))
  src <- csv_tsibble_source(dir, index = t, col_types = list(t = as.Date))
  write_shard(dir, "part-1.csv", "t,v\n2020-01-01,1\n2020-01-02,2\n")
  seal(dir, "part-1.csv")
  before <- poll(src)

  # missing final line terminator
  write_shard(dir, "part-2.csv", "t,v\n2020-01-03,3\n2020-01-04,4")
  seal(dir, "part-2.csv")
  expect_error(poll(src), "terminated record")
  expect_identical(snapshot(src), before)

  # once fixed, the same shard is accepted
  write_shard(dir, "part-2.csv", "t,v\n2020-01-03,3\n2020-01-04,4\n")
  seal(dir, "part-2.csv")
  after <- poll(src)
  expect_equal(NROW(after), 4L)
  expect_equal(format(tsibble::interval(after)), "1D")
})

test_that("the fixed header is enforced across shards", {
  dir <- tempfile("csv-src-"); dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))
  src <- csv_tsibble_source(dir, index = t, col_types = list(t = as.Date))
  write_shard(dir, "part-1.csv", "t,v\n2020-01-01,1\n")
  seal(dir, "part-1.csv")
  poll(src)

  write_shard(dir, "part-2.csv", "t,w\n2020-01-02,2\n")
  seal(dir, "part-2.csv")
  expect_error(poll(src), "Fixed header mismatch")
  expect_equal(NROW(snapshot(src)), 1L)
})

test_that("cumulative key/index duplicates retain the previous data", {
  dir <- tempfile("csv-src-"); dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))
  src <- csv_tsibble_source(dir, index = t, key = g,
                            col_types = list(t = as.Date))
  write_shard(dir, "part-1.csv", "t,g,v\n2020-01-01,a,1\n2020-01-02,a,2\n")
  seal(dir, "part-1.csv")
  poll(src)

  write_shard(dir, "part-2.csv", "t,g,v\n2020-01-02,a,99\n")
  seal(dir, "part-2.csv")
  expect_error(poll(src), "duplicate")
  expect_error(poll(src), "duplicate") # still rejected, no accumulation
  expect_equal(NROW(snapshot(src)), 2L)

  # non-strict mode warns instead of erroring
  expect_warning(poll(src, strict = FALSE), "duplicate")
  expect_equal(NROW(snapshot(src)), 2L)
})

test_that("fixed column types cannot silently drift", {
  dir <- tempfile("csv-src-"); dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))
  src <- csv_tsibble_source(dir, index = t, col_types = list(t = as.Date))
  write_shard(dir, "part-1.csv", "t,v\n2020-01-01,1\n2020-01-02,2\n")
  seal(dir, "part-1.csv")
  expect_type(poll(src)$v, "integer")

  write_shard(dir, "part-2.csv", "t,v\n2020-01-03,hello\n")
  seal(dir, "part-2.csv")
  expect_error(poll(src), "fixed type|cannot be parsed")
  expect_equal(NROW(snapshot(src)), 2L)
})

test_that("returned snapshots are not mutated by later polls", {
  dir <- tempfile("csv-src-"); dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))
  src <- csv_tsibble_source(dir, index = t, col_types = list(t = as.Date))
  write_shard(dir, "part-1.csv", "t,v\n2020-01-01,1\n")
  seal(dir, "part-1.csv")
  old <- poll(src)

  write_shard(dir, "part-2.csv", "t,v\n2020-01-02,2\n")
  seal(dir, "part-2.csv")
  new <- poll(src)
  expect_equal(NROW(old), 1L)
  expect_equal(NROW(new), 2L)
  expect_false(identical(old, new))
})

test_that("future data comes from the current data and rebuilt interval", {
  dir <- tempfile("csv-src-"); dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))
  src <- csv_tsibble_source(dir, index = t, col_types = list(t = as.Date))
  write_shard(dir, "part-1.csv", "t,v\n2020-01-01,1\n2020-01-02,2\n")
  seal(dir, "part-1.csv")
  poll(src)
  fut <- generate_future(src, n = 2L)
  expect_equal(fut$t, as.Date(c("2020-01-03", "2020-01-04")))
})

test_that("an unknown interval is reported explicitly and blocks futures", {
  dir <- tempfile("csv-src-"); dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))
  src <- csv_tsibble_source(dir, index = t, col_types = list(t = as.Date))
  write_shard(dir, "part-1.csv", "t,v\n2020-01-01,1\n")
  seal(dir, "part-1.csv")
  tsbl <- poll(src)
  expect_equal(format(tsibble::interval(tsbl)), "?")
  expect_error(generate_future(src), "interval is unknown")
})

test_that("out-of-order sealing is rejected and publishes nothing", {
  dir <- tempfile("csv-src-"); dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))
  src <- csv_tsibble_source(dir, index = t, col_types = list(t = as.Date))
  write_shard(dir, "part-1.csv", "t,v\n2020-01-01,1\n")
  write_shard(dir, "part-2.csv", "t,v\n2020-01-02,2\n")
  seal(dir, "part-2.csv") # part-1 still open
  expect_error(poll(src), "sequence order")
  expect_null(snapshot(src))
})

test_that("completion needs both the end marker and every shard sealed", {
  dir <- tempfile("csv-src-"); dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))
  src <- csv_tsibble_source(dir, index = t, col_types = list(t = as.Date))
  write_shard(dir, "part-1.csv", "t,v\n2020-01-01,1\n")
  file.create(file.path(dir, "_END"))
  expect_false(is_complete(src))      # producer ended, shard still open
  seal(dir, "part-1.csv")
  expect_true(is_complete(src))
  expect_equal(NROW(poll(src)), 1L)
})
