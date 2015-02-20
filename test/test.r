#library("teradataR")
library("RODBC")
library("dplyr")
library("assertthat")


#con <- tdConnect(dsn, uid = uid, pwd = pwd, database = database)
#a <- td.data.frame(test_table)
# tdQuery('select count(*) from airlines')
#tdClose()

#a1 <- as.td.data.frame(a, tableName = "airlines", database = "")

st <- src_teradata(host = dsn, user = uid, password = pwd, dbname = "")
a <- tbl(st, "airlines")

copy_nycflights13(st)
#copy_nycflights13(src_teradata(host = dsn, user = uid, password = pwd))
#copy_lahman(src_teradata(host = dsn, user = uid, password = pwd))

getS3method("collapse","tbl_sql")

a <- td.table(st$con,table = "airlines",database = "")

collapse(select(a))

a <- tbl(st, "airlines")
