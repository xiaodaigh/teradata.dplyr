#' Connect to a Teradata database using ODBC
#'
#' Use \code{src_teradata} to connect to an existing Teradata database,
#' and \code{tbl} to connect to tables within that database.
#' If you are running a local Teradataql database, leave all parameters set as
#' their defaults to connect. If you're connecting to a remote database,
#' ask your database administrator for the values of these variables.
#'
#' @param path Path to Teradata database
#' @param create if \code{FALSE}, \code{path} must already exist. If
#'   \code{TRUE}, will create a new Teradata database at \code{path}.
#' @param src a Teradata src created with \code{src_teradata}.
#' @param from Either a string giving the name of table in database, or
#'   \code{\link{sql}} described a derived table or compound join.
#' @param ... Included for compatibility with the generic, but otherwise
#'   ignored.
#' @examples
#' 
#' \dontrun{
#' # to use the Batting dataset
#' library(Lahman) # define your own dsn, uid, pwd, and database con.td <- 
#' src_teradata(dsn = dsn, uid = uid, pwd = pwd, database = database) # Methods 
#' ------------------------------------------------------------------- copying 
#' data from R into Teradata is inefficient. Please use standard Teradata tools 
#' instead reduce to first 1000 rows to illustrate this example
#' Batting1000 <- Batting[1:1000,]
#' batting <- copy_to(con.td,Batting1000)
#' dim(batting)
#' colnames(batting)
#' head(batting)
#'
#' # Data manipulation verbs ---------------------------------------------------
#' filter(batting, yearID > 2005, G > 130)
#' select(batting, playerID:lgID)
#' arrange(batting, playerID, desc(yearID))
#' summarise(batting, G = mean(G), n = n())
#' mutate(batting, rbi2 = 1.0 * R / AB)
#'
#' # note that all operations are lazy: they don't do anything until you
#' # request the data, either by `print()`ing it (which shows the first ten
#' # rows), by looking at the `head()`, or `collect()` the results locally.
#'
#' system.time(recent <- filter(batting, yearID > 2010))
#' system.time(collect(recent))
#'
#' # Group by operations -------------------------------------------------------
#' # To perform operations by group, create a grouped object with group_by
#' players <- group_by(batting, playerID)
#' group_size(players)
#'
#' # sqlite doesn't support windowed functions, which means that only
#' # grouped summaries are really useful:
#' summarise(players, mean_g = mean(G), best_ab = max(AB))
#'
#' # When you group by multiple level, each summarise peels off one level
#' per_year <- group_by(batting, playerID, yearID)
#' stints <- summarise(per_year, stints = max(stint))
#' filter(ungroup(stints), stints > 3)
#' summarise(stints, max(stints))
#'
#' # Joins ---------------------------------------------------------------------
#' sql_join_statement<- "select a.*, b.* from table1 as a, table2 as b where a.id = b.id"
#' sqlQuery(con.td, sql_join_statement)
#'
#' # Match players and their hall of fame data
#' # none of these work use the above to join datasets!
#' # inner_join(player_info, hof)
#' # Keep all players, match hof data where available
#' # left_join(player_info, hof)
#' # Find only players in hof
#' # semi_join(player_info, hof)
#' # Find players not in hof
#' # anti_join(player_info, hof)
#'
#' # Arbitrary SQL -------------------------------------------------------------
#' sql_join_statement<- "select a.*, b.* from table1 as a, table2 as b where a.id = b.id"
#' sqlQuery(con.td, sql_join_statement)
#' }
#' @import RODBC
#' @export
src_teradata <- function(dsn = NULL, database = NULL, uid = NULL, pwd = NULL, port = NULL, ...) {
  tdConnection <- NULL
  
  st <- paste0("DSN=", dsn)
  if (nchar(uid)) 
    st <- paste0(st, ";UID=", uid)
  if (nchar(pwd)) 
    st <- paste0(st, ";PWD=", pwd)
  if (nchar(database)) 
    st <- paste0(st, ";Database=", database)
  tdConnection <- odbcDriverConnect(st, ...)
  
  src <- src_sql("teradata", tdConnection, database = database)
  # By default the source of the connection is RODBC need to add a class
  class(src$con) <- c("RODBC.teradata", class(src$con))
  src
}

#' @export
src_desc.src_teradata <- function(xx) {
  x <- xx$con
  # code adapted from print.odbc of the RODBC package
  con <- strsplit(attr(x, "connection.string"), ";", fixed = TRUE)[[1L]]
  case <- paste("case=", attr(x, "case"), sep = "")
  cat("Teradata RODBC Connection ", as.vector(x), "\nDetails:\n  ", sep = "")
  cat(case, con, sep = "\n  ")
  invisible(x)
}

#' @export
db_list_tables.RODBC.teradata <- function(con) {
  sqlTables(con)
}

#' @export
db_has_table.RODBC.teradata <- function(con, table) {
  tmp <- sqlTables(con)$TABLE_NAME
  table %in% tmp
}

#' @export
src_translate_env.src_teradata <- function(x) {
  # src_translate_env.tbl_sql(x)
  sql_variant(base_scalar, 
              sql_translator(.parent = base_agg 
                , n = function() sql("count(*)")
                , sd = sql_prefix("stddev_pop")
                , skewness = sql_prefix("skew")
                , cor = sql_prefix("corr")
                , var = sql_prefix("var_pop")
                , cov = sql_prefix("covar_pop")
                ), 
              base_win)
}

#' @export
sql_escape_ident.RODBC.teradata <- function(con, x) {
  sql_quote(x, "\"")
}


#' @export
tbl.src_teradata <- function(src, from, ...) {
   tbl_sql("teradata", src,from,...)
}

#' @export
db_query_fields.RODBC.teradata <- function(con, sql, ...) {
  fields <- build_sql("SELECT * FROM ", sql, " Sample 0", 
                      con = con)
  qry <- sqlQuery(con, fields)
  names(qry)
}

#' @importFrom plyr compact
#' @import assertthat
#' @export 
sql_select.RODBC.teradata <- function (con, select, from, where = NULL, group_by = NULL, having = NULL, 
                                       order_by = NULL, limit = NULL, offset = NULL, ...) {
  #' code adpated from dplyr
  out <- vector("list", 8)
  names(out) <- c("select", "from", "where", "group_by", "having", 
                  "order_by", "limit", "offset")
  assert_that(is.character(select), length(select) > 0L)
  out$select <- build_sql("SELECT ", escape(select, collapse = ", ", 
                                            con = con))
  assert_that(is.character(from), length(from) == 1L)
  out$from <- build_sql("FROM ", from, con = con)
  if (length(where) > 0L) {
    assert_that(is.character(where))
    out$where <- build_sql("WHERE ", escape(where, collapse = " AND ", 
                                            con = con))
  }
  if (!is.null(group_by)) {
    assert_that(is.character(group_by), length(group_by) > 
                  0L)
    out$group_by <- build_sql("GROUP BY ", escape(group_by, 
                                                  collapse = ", ", con = con))
  }
  if (!is.null(having)) {
    assert_that(is.character(having), length(having) == 1L)
    out$having <- build_sql("HAVING ", escape(having, collapse = ", ", 
                                              con = con))
  }
  if (!is.null(order_by)) {
    assert_that(is.character(order_by), length(order_by) > 
                  0L)
    out$order_by <- build_sql("ORDER BY ", escape(order_by, 
                                                  collapse = ", ", con = con))
  }
  if (!is.null(limit)) {
    assert_that(is.integer(limit), length(limit) == 1L)
    out$limit <- build_sql("SAMPLE ", limit, con = con)
  }
  if (!is.null(offset)) {
    assert_that(is.integer(offset), length(offset) == 1L)
    out$offset <- build_sql("OFFSET ", offset, con = con)
  }
  escape(unname(compact(out)), collapse = "\n", parens = FALSE, 
         con = con)
}

#' @export
query.RODBC.teradata <- function(con, sql, .vars) {
  # return an object with embedded functions
  Teradata.Query$new(con, sql, .vars)
}

#' @export
sql_subquery.RODBC.teradata <- function(con, sql, name =unique_name(), 
                                        ...) {
  # funciton taken from dplyr
  if (is.ident(sql)) 
    return(sql)
  build_sql("(", sql, ") AS ", ident(name), con = con)
}


#' @export
tbl_vars.tbl_teradata <- function(x) {
  as.character(x$select)
}

#' Copy a local data frame to a remote src.
#' 
#' WARNING: This suffers from obvious performance issues. Not recommended for
#' large datasets This uploads a local data frame into a remote data source,
#' creating the table definition as needed. Wherever possible, the new object
#' will be temporary, limited to the current connection to the source.

#' 
#' @param dest remote data source
#' @param df local data frame
#' @param name name for new remote table.
#' @param database the default database to connect to
#' @param ... other parameters passed to methods.
#' @return a \code{tbl} object in the remote source
#' @import assertthat
#' @export
#' @seealso \code{\link[dplyr]{copy_to}}
copy_to.src_teradata <- function(dest, df, name = deparse(substitute(df)), 
                                 database = dest$database, ...) {
  warning("The copy_to function for Teradata suffers from performance issues. If you need to upload large data to Teradata you are highly recommended to use the standard tools that Teradata provides")
  # dest is expected to have a $con which should be the output of
  # src_teradata ZJ: worry about the dest part later as I need to totally
  # rip up the teradataR to do that
  assert_that(is.data.frame(df), is.string(name))
  
  x <- df
  # the below code is adapted from tdSave of the teradataR package
  # tdSave(df, name)
  if (inherits(x, "td.data.frame") | inherits(x, "td.table")) {
    return(x)
  } else if (inherits(x, "data.frame")) {
    tablename <- name
    if (nchar(tablename) > 0) 
      tbl <- tablename else tbl <- deparse(substitute(x))
      if ("RODBC.teradata" %in% class(dest$con)) {
        # drop the table first if it already exists
        if (db_has_table(dest$con, tbl)) {
          if (nchar(database)) 
            sqlQuery(dest$con, sprintf("drop table %s.%s", database, 
                                       tbl)) else sqlQuery(dest$con, sprintf("drop table %s", tbl))
        }
        
        # zJ: it would be disaster if the nrows is large and the first column
        # has only a few values aim to have each column on less than 10k
        # manually create a id
        if (any(table(x[[1]]) > 10000)) {
          x <- cbind(rep(1:(nrow(x)/10000), length.out = nrow(x)), 
                     x)
        }
        
        if (nchar(database)) 
          sqlSave(dest$con, x, tablename = paste0(database, ".", 
                                                  tbl)) else sqlSave(dest$con, x, tablename = tbl)
        
        return(tbl(dest,tbl))
      }
  }
}


#' @export
db_save_query.RODBC.teradata <- function(con, sql, name, temporary = TRUE, 
                                         ...) {
  if (temporary) {
    # firstly create a volatile table which actually contains some
    volatile_name <- ident(paste0(name, "v"))
    tt_sql1 <- build_sql(sql("CREATE MULTISET VOLATILE TABLE "), volatile_name, 
                         " AS (", sql, ") with data on commit preserve rows", con = con)
    # this will create the volatile table
    sqlQuery(con, tt_sql1)
    
    tt_sql2 <- build_sql(sql("CREATE MULTISET GLOBAL TEMPORARY TABLE "), 
                         ident(name), " AS (", sql, ") with no data on commit preserve rows", 
                         con = con)
    # in Teradata a global temporary table is nothing but a shell; it
    # doesn't contain any data so need to insert data into
    sqlQuery(con, tt_sql2)
    # need to insert the volatile table into the temporary table
    sqlQuery(con, sprintf("insert into %s select * from %s", ident(name), 
                          volatile_name))
    
    # You CAN'T use sqlDrop(con, volatile_name) to drop the volatile table as the odbc function will check if it exists
    # which it think doesn't since it's a volatile table in Teradata and will error
    sqlQuery(con, sprintf("drop table %s", volatile_name))
    
  } else {
    tt_sql2 <- build_sql("CREATE TABLE ", ident(name), " AS (", sql, 
                         ") with no data on commit preserve rows", con = con)
    res <- sqlQuery(con, tt_sql2)
  }

  name
}


#' @export
sql_escape_string.RODBC.teradata <- function(con, x) {
  sql_quote(x, "'")
}

#' @export
sql_join.RODBC.teradata <- function (con, x, y, type = "inner", by = NULL, ...)  {
  join <- switch(type, left = sql("LEFT"), inner = sql("INNER"), 
                 right = sql("RIGHT"), full = sql("FULL"), stop("Unknown join type:", 
                                                                type, call. = FALSE))
  by <- common_by(by, x, y)
  using <- all(by$x == by$y)
  x_names <- auto_names(x$select)
  y_names <- auto_names(y$select)
  uniques <- unique_names(x_names, y_names, by$x[by$x == by$y])
  if (is.null(uniques)) {
    sel_vars <- c(x_names, y_names)
  }
  else {
    x <- update(x, select = setNames(x$select, uniques$x))
    y <- update(y, select = setNames(y$select, uniques$y))
    by$x <- unname(uniques$x[by$x])
    by$y <- unname(uniques$y[by$y])
    sel_vars <- unique(c(uniques$x, uniques$y))
  }
  if (using) {
    cond <- build_sql("USING ", lapply(by$x, ident), con = con)
  }
  else {
    on <- sql_vector(paste0(sql_escape_ident(con, by$x), 
                            " = ", sql_escape_ident(con, by$y)), collapse = " AND ", 
                     parens = TRUE)
    cond <- build_sql("ON ", on, con = con)
  }
  from <- build_sql("SELECT * FROM ", sql_subquery(con, x$query$sql), 
                    "\n\n", join, " JOIN \n\n", sql_subquery(con, y$query$sql), 
                    "\n\n", cond, con = con)
  attr(from, "vars") <- lapply(sel_vars, as.name)
  from
}

#' @export
left_join.tbl_teradata <- function(...) warning("Not implemented use RODBC::sqlQuery(src$con, sql_statement)")

#' @export
semi_join.tbl_teradata <- function(...) warning("Not implemented use RODBC::sqlQuery(src$con, sql_statement)")


#' @export
inner_join.tbl_teradata <- function(...) warning("Not implemented use RODBC::sqlQuery(src$con, sql_statement)")


#' @export
anti_join.tbl_teradata <- function(...) warning("Not implemented use RODBC::sqlQuery(src$con, sql_statement)")

#' @import dplyr
sql_set_op.RODBC.teradata <- dplyr:::sql_set_op.DBIConnection

