process_file <- function(filename) {
  lines <- 0
  words <- 0
  characters <- 0

  con <- file(filename, "r")
  while (TRUE) {
    line = readLines(con, n = 1)
    if ( length(line) == 0 ) {
      break
    }
    lines <- lines + 1
    line <- strsplit(line, '\\s+')[[1]]
    if (length(line) != 0) {
      words <- words + length(line)
      characters <- characters + sum(sapply(line, nchar))
    }
  }
  close(con)

  list(
    metadata=list(
      lines=lines,
      words=words,
      characters=characters
    )
  )
}
