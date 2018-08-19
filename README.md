ALExQin
2018

A connection pool for mysql, work with mysql_connnector
cs1 = server_info(host='127.0.0.1', user='dbuser', password='dbpassword', database='dbname')
cs2 = dict(host='127.0.0.1', user='dbuser', password='dbpassword', database='dbname')
con_pool = com_con('poolname', cs1 or cs2, length=10, flexible=False)

By default flexible is set to False, in this mode, mysql connection will be created at the beggining.
If flexible is set to True, no connection created until use it.

using by:
1. quick and simple:
con_pool['show tables']

2. take and release
con = con_pool.free()
con.query_db('show tables')
con_pool.release(con)

3. with mode
with con_pool as con:
	con.execute_db('insert into table values(1,2,3)')