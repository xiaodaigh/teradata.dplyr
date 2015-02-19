#' @import teradataR
#' @export
#' @example
src_teradata <- function(dbname = NULL, host = NULL, port = NULL, user = NULL,
                         password = NULL, dType = c("odbc", "jdbc"), database = "", ...) {

  tdConnection <- NULL
  dsn <- host
  uid <- user
  pwd <- password
  dType <- match.arg(dType)

  # code taken from teradataR
  if (dType == "odbc") {
    require(RODBC)
    st <- paste("DSN=", dsn, sep = "")
    if (nchar(uid))
      st <- paste(st, ";UID=", uid, sep = "")
    if (nchar(pwd))
      st <- paste(st, ";PWD=", pwd, sep = "")
    if (nchar(database))
      st <- paste(st, ";Database=", database, sep = "")
    tdConnection <- odbcDriverConnect(st, ...)
    # don't mess with the global environment
    #assign("tdConnection", tdConnection, envir = .GlobalEnv)
    #tdConnection
  } else if (dType == "jdbc") {
    # ZJ: the jdbc part isn't implemented yet
    stopifnot(dType != "jdbc")

    require(RJDBC)
    drv <- JDBC("com.teradata.jdbc.TeraDriver")
    st <- paste("jdbc:teradata://", dsn, sep = "")
    if (nchar(database))
      st <- paste(st, "/database=", database, sep = "")
    tdConnection <- dbConnect(drv, st, user = uid, password = pwd, ...)
    # don't mess with the global environment
    #assign("tdConnection", tdConnection, envir = .GlobalEnv)
    #tdConnection
  }


  src_sql("teradata", tdConnection)
}

#' @export
src_desc.src_teradata <- function(xx) {
  x <- xx[[1]]
  # code taken from print.odbc of the RODBC package
  con <- strsplit(attr(x, "connection.string"), ";", fixed = TRUE)[[1L]]
  case <- paste("case=", attr(x, "case"), sep="")
  cat("RODBC Connection ", as.vector(x), "\nDetails:\n  ", sep = "")
  cat(case, con, sep="\n  ")
  invisible(x)
}

#' @export
db_list_tables.RODBC <- function(con) {
  sqlTables(con)
}

db_list_tables.src_teradata <- db_list_tables.RODBC

#' @export
db_has_table.RODBC <- function(con, table) {
  tmp <- sqlTables(con)$TABLE_NAME
  table %in% tmp
}

db_has_table.src_teradata <- db_has_table.RODBC

#' @export
tbl.src_teradata <- function(src, from, ...) {
  tbl_sql("teradata", src = src, from = from, ...)
}
