#' The teradata.query object mirroring dplyr's Query object for DBI connections
#' 
Teradata.Query <- R6::R6Class("Teradata.Query",
                     private = list(
                       .nrow = NULL,
                       .vars = NULL
                     ),
                     public = list(
                       con = NULL,
                       sql = NULL,
                       
                       initialize = function(con, sql, vars) {
                         self$con <- con
                         self$sql <- sql
                         private$.vars <- vars
                       },
                       
                       print = function(...) {
                         cat("<Query> ", self$sql, "\n", sep = "")
                         print(self$con)
                       },
                       
                       fetch = function(n) {
                         res <- sqlQuery(self$con, self$sql)
                         res
                       },
                       
                       fetch_paged = function(chunk_size = 1e4, callback) {
                         warning("This package does not support fetched_paged for Teradata")
#                          qry <- dbSendQuery(self$con, self$sql)
#                          on.exit(dbClearResult(qry))
#                          
#                          while (!dbHasCompleted(qry)) {
#                            chunk <- fetch(qry, chunk_size)
#                            callback(chunk)
#                          }
                         
                         invisible(TRUE)
                       },
                       
                       vars = function() {
                         private$.vars
                       },
                       
                       nrow = function() {
                         count_rows_sql <- sprintf("SELECT CAST(COUNT(*) AS FLOAT) FROM (%s) as tmp;", self$sql)
                         as.numeric(sqlQuery(self$con, count_rows_sql))
                       },
                       
                       ncol = function() {
                         length(self$vars())
                       }
                     )
)
