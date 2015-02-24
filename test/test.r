#library("teradataR")
library("RODBC")
library("dplyr")
library("assertthat")


#con <- tdConnect(dsn, uid = uid, pwd = pwd, database = database)
#a <- td.data.frame(test_table)
# tdQuery('select count(*) from udparm.ao_sovn')

#td.stats(a, "Delinquency")
#tdClose()

#a1 <- as.td.data.frame(a, tableName = "ao_sovn2", database = "udparm")

st <- src_teradata(dsn = dsn, uid = uid, pwd = pwd)
a <- tbl(st, "airlines")
dim(a)
collect(a)
collapse(a)
compute(a)

a1 <- filter(a, carrier == "AA")
collect(a1)
collapse(a1)
compute(a1)
dim(a1)

