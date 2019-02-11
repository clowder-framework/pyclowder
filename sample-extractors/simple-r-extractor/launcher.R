#!/usr/bin/env Rscript

# get environment variables
r_script <- Sys.getenv('R_SCRIPT')
r_function <- Sys.getenv('R_FUNCTION')
if (r_function == '') {
  stop("Need a function to call.")
}

# command line arguments
args <- commandArgs(trailingOnly=TRUE)
if (length(args) != 2) {
  stop("Need 2 arguments (input_file, json_file)")
}
input_file <- args[1]
json_file <- args[2]

# source script file
if (r_script != '') {
  source(r_script)
}

# call function
result <- do.call(r_function, list(input_file))

# write result as json
write(jsonlite::toJSON(result, auto_unbox=TRUE), json_file)
