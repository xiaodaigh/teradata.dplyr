#library("teradataR")
# library("RODBC")
# library("dplyr")
# library(data.table)
# library("assertthat")


#' td.table - a R reference to Teradata table
td.table <- function(con, table, database = "") {
  if (missing(database) || is.null(database) || nchar(database) == 0) {
    obj <- gettextf("\"%s\"", table)
  } else obj <- gettextf("\"%s\".\"%s\"", database, table)
  query <- gettextf("SELECT * FROM %s SAMPLE 0", obj)
  res <- try(sqlQuery(con, query))
  if (is.null(attr(res, "class"))) {
    res <- data.table()
    attr(res, "totalRows") <- 0
    warning("Teradata table not found.  Result is empty data frame.")
  } else {
    query <- sprintf("SELECT CAST(COUNT(*) AS FLOAT) FROM %s", obj)
    res2 <- try(sqlQuery(con, query))
    attr(res, "totalRows") <- as.numeric(res2)
  }
  attr(res, "class") <- "td.table"
  attr(res, "tableName") <- table
  if (!is.null(database) && !missing(database) && nchar(database) > 0) {
    attr(res, "database") <- database
  } else {
    res2 <- try(sqlQuery(con, "SELECT DATABASE"))
    if (!is.null(attr(res2, "class")))
      attr(res, "database") <- as.character(res2[[1]])
  }
  return(res)
}

#' A src_teradata based on ODBC
#' @import RODBC
#' @export
#' @example
#' src_teradata(host = dsn, user = uid, password = pwd)
src_teradata <- function(host = NULL, dbname = NULL, port = NULL, user = NULL, password = NULL,  ...) {
  tdConnection <- NULL
  dsn <- host
  uid <- user
  pwd <- password
  database <- dbname

  # code adapted from teradataR's tdConnect function
  st <- paste0("DSN=", dsn)
  if (nchar(uid))
    st <- paste0(st, ";UID=", uid)
  if (nchar(pwd))
    st <- paste0(st, ";PWD=", pwd)
  if (nchar(database))
    st <- paste0(st, ";Database=", database)
  tdConnection <- odbcDriverConnect(st, ...)

  src <- src_sql("teradata", tdConnection)
  attr(src, "database") <- database
  #class(src$con) <- c(class(src$con),"tbl_sql")
  class(src) <- c(class(src),"tbl_sql")
  src
}

#' @export
src_desc.src_teradata <- function(xx) {
  x <- xx[[1]]
  # code taken from print.odbc of the RODBC package
  con <- strsplit(attr(x, "connection.string"), ";", fixed = TRUE)[[1L]]
  case <- paste("case=", attr(x, "case"), sep = "")
  cat("RODBC Connection ", as.vector(x), "\nDetails:\n  ", sep = "")
  cat(case, con, sep = "\n  ")
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

sql_escape_ident.RODBC <- function (con, x) {
  sql_quote(x, "\"")
}


#' @export
tbl.src_teradata <- function(src, from, ...) {
  tbl_sql("teradata", src = src, from = from, ...)
}

tbl.RODBC <- tbl.src_teradata

db_query_fields.RODBC <- function(con, sql,...) {
  RODBC::sqlColumns(con, sql ,...)$COLUMN_NAME
}

sql_select.RODBC <- dplyr:::sql_select.DBIConnection

query.RODBC <- function(con, sql, vars) {
  RODBC::sqlQuery(con, sql)
}

#' @import assertthat
copy_to.src_teradata <- function(dest, df, name = deparse(substitute(df)), database = attr(dest,"database"), ...) {
  # dest is expected to have a $con which should be the output of
  # src_teradata ZJ: worry about the dest part later as I need to totally
  # rip up the teradataR to do that
  assert_that(is.data.frame(df), is.string(name))

  x <- df
  # the below code is adapted from tdSave of the teradataR package tdSave(df, name)
  if (inherits(x, "td.data.frame") | inherits(x, "td.table")) {
    return(x)
  } else if (inherits(x, "data.frame")) {
    tablename <- name
    if (nchar(tablename) > 0)
      tbl <- tablename
    else
      tbl <- deparse(substitute(x))
    if (class(dest$con) == "RODBC") {
      # drop the table first if it already exists
      if(db_has_table(dest$con,tbl)) {
        if(nchar(database))
          sqlQuery(dest$con, sprintf("drop table %s.%s",database,tbl))
        else
          sqlQuery(dest$con, sprintf("drop table %s",tbl))
      }
      
      # zJ: it would be disaster if the nrows is large and the first column has only a few values
      # aim to have each column on less than 10k
      # manually create a id
      if (any(table(x[[1]]) > 10000)) {
        x <- cbind(rep(1:(nrow(x)/10000), length.out = nrow(x)), x)
      }
      
      if(nchar(database))
        sqlSave(dest$con, x, tablename = paste0(database,".", tbl))
      else
        sqlSave(dest$con, x, tablename = tbl)

      return(td.table(dest$con,table = tbl, database ))
    }
  }
}
