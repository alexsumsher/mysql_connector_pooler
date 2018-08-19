# -*- coding:utf-8
"""
MySQL Database Connection.
change to fit for poolo class.
last update: 20171229.
update: com_con,增加flexiable模式的pool

20180103: com_con, __enter__ with elock(thread.Lock) for thread safe
201803：new arg for execute_db: get_ins_id(false), weather return last insert id
"""
import mysql.connector as mcr
import mysql.connector.pooling as mcrp
import logging
import time
#   for com_con
from random import random as rdm
from threading import Lock
logging.basicConfig(level=logging.DEBUG, format='(%(funcName)-10s) %(message)s')


class server_info:
    server_info = {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'dbuser',
        'password': 'dbpassword',
        'database': 'dbname',
        'charset': 'utf8'
    }

    def __init__(self, **configure):
        self.D = dict()
        for _ in self.__class__.server_info.iterkeys():
            self.D[_] = configure.get(_) or self.__class__.server_info[_]

    def __getitem__(self, item):
        return self.D.get(item)

    def __setitem__(self, n, v):
        if n in self.D:
            self.D[n] = v

    @property
    def info(self):
        return str(self.D)


class com_con(object):
    length = 10
    dead_len = 30
    recover_time = 60
    #   w is a mark for pool
    w = 'pool'

    @classmethod
    def set_deadlen(cls, dlen=0):
        if dlen > cls.dead_len:
            cls.dead_len = dlen
            return dlen
        else:
            return cls.dead_len

    #   -1: not inited; 1: working; 0:shutdown
    def __init__(self, con_info, length=0, atonce=True, flexible=False):
        # if flexible, work with lenth and dead_length, and self.c->last time work mode, if add new con is set to 1, if kick set to -1: 
        # when take: if overlen create new con, if over deadlen error
        # when kick: if finger > len and last time is kick, will not append to conlist for reuse, just remove it(on the other hand, if the last time action is still create new con, which means the pool may still works under busy mode)
        self.length = length or self.__class__.length
        if flexible:
            if self.length < self.__class__.dead_len:
                self.dead_len = self.__class__.dead_len
                self.__take_kick = self.__take_kick_2
                self.c = 0
            else:
                logging.warning('length is bigger than dead_len, will not work in flexible mode!')
                self.dead_len = 0
                self.__take_kick = self.__take_kick_1
        else:
            self.dead_len = 0
            self.__take_kick = self.__take_kick_1
        self.cif = con_info if isinstance(con_info, dict) else con_info.D if isinstance(con_info, server_info) else None
        if self.cif is None:
            raise RuntimeError("NO connection info!")
        self.finger = 0
        self.ilock = Lock()
        self.elock = Lock()
        self.status = -1
        self.conlist = []
        self.staticlist = []
        self.curcon = None
        self.recover_time = self.__class__.recover_time
        self.w = 'pool'
        if not atonce:
            return
        self.ilock.acquire()
        try:
            self.__inilist()
        finally:
            self.ilock.release()

    def __getitem__(self, sqlcmds):
        if len(sqlcmds) > sqlcmds.index(';') + 1:
            return self.do_sequence(sqlcmds)
        else:
            operation = sqlcmds.split(' ')[0]
            if operation.lower() in ('insert', 'update', 'delete'):
                return self.execute_db(sqlcmds)
            else:
                rt = self.query_db(sqlcmds)
                return rt[0] if rt and len(rt) == 1 else rt

    def __inilist(self):
        if self.status > 0:
            return
        if len(self.conlist) > 0:
            for i in xrange(len(self.conlist)):
                self.conlist.pop().close()
        if len(self.staticlist) > 0:
            for i in xrange(len(self.staticlist)):
                self.staticlist.pop().close()
        if self.status == -1:
            self.staticlist = [None] * self.length
            for i in xrange(self.length):
                con = mcr.connect(**self.cif)
                con.mark = 0
                self.conlist.append(con)
                self.staticlist[i] = con
                time.sleep(0.05)
        elif self.status == 0:
            i = 0
            for con in self.staticlist:
                t = con.connect()
                con.mark = 0
                if t == 0:
                    self.conlist.remove(con)
                    self.conlist.append(mcr.connect(**self.cif))
                    del con
                i += 1
            if i < self.length:
                for x in xrange(self.length - i):
                    con = mcr.connect(**self.cif)
                    con.mark = 0
                    self.conlist.append(con)
            self.staticlist = [None] * self.length
            for t in xrange(len(self.conlist)):
                self.staticlist[t] = self.conlist[t]
        self.status = 1
        return self.status

    def shutdown(self):
        self.ilock.acquire()
        for con in self.staticlist:
            con.close()
            time.sleep(0.05)
        self.status = 0
        self.ilock.release()

    def __batch_recovery(self):
        ctime = int(time.time())
        for con in self.staticlist:
            if ctime - con.mark >= self.recover_time:
                con.mark = 0
                self.conlist.append(con)
                self.finger -= 1
        return self.finger

    def __str__(self):
        return 'status: %s\tfinger: %s\t; usage: %s/%s' % (self.status, self.finger, len(self.staticlist), len(self.conlist))

    def __take_kick_2(self, con=None):
        logging.debug('status: %s\tfinger: %s\t; usage: %s/%s' % (self.status, self.finger, len(self.staticlist), len(self.conlist)))
        #   work on flexible mode
        def newcon():
            ncon = mcr.connect(**self.cif)
            if ncon:
                # create con and direct use, so it's no need to append to conlist
                self.staticlist.append(ncon)
                self.finger += 1
                self.c = 1
                return ncon
            else:
                return None

        self.ilock.acquire()
        if con:
            if self.finger > self.length and self.c < 0:
                self.staticlist.remove(con)
                del con
            else:
                self.conlist.append(con)
                con.mark = 0
            self.c = -1
            self.finger -= 1
            self.ilock.release()
            return self.finger
        if self.status == 0:
            if self.__inilist() != 1:
                self.ilock.release()
                raise RuntimeError('Cannot Initial the Pool!')
        elif self.status == -1:
            self.conlist = []
            self.staticlist = []
            con = newcon()
            self.ilock.release()
            if con:
                self.status = 1
                return con
            else:
                return RuntimeError('Not able to inital the pool!')
        if self.finger >= self.dead_len:
            if self.__batch_recovery() >= self.dead_len:
                self.ilock.release()
                raise RuntimeError('Work on flexible Mode and over dead_len!')
        elif self.finger >= self.length:
            con = newcon()
            self.ilock.release()
            return con
        con = self.conlist.pop(0)
        self.finger += 1
        self.c = 1
        self.ilock.release()
        return con

    def __take_kick_1(self, con=None):
        logging.debug('status: %s\tfinger: %s\t; usage: %s/%s' % (self.status, self.finger, len(self.staticlist), len(self.conlist)))
        self.ilock.acquire()
        if con:
            self.conlist.append(con)
            con.mark = 0
            self.finger -= 1
            self.ilock.release()
            return self.finger
        if self.status <= 0:
            if self.__inilist() != 1:
                self.ilock.release()
                raise RuntimeError('Cannot Initial the Pool!')
        if self.finger >= self.length:
            self.__batch_recovery()
            if self.finger >= self.length:
                for i in xrange(5):
                    time.sleep(round(rdm(),1))
                    if self.finger < self.length:
                        break
                if i >= 4:
                    self.ilock.release()
                    return None
        con = self.conlist.pop(0)
        self.finger += 1
        self.ilock.release()
        return con

    def free(self):
        con = self.__take_kick()
        if con:
            con.mark = int(time.time())
            return con
        else:
            return None

    def release(self, con=None):
        if con:
            return self.__take_kick(con)
        else:
            return self.__batch_recovery()

    def __enter__(self):
        self.elock.acquire()
        if self.curcon is None:
            logging.info('con for with is still None, create it!')
            self.curcon = mcr.connect(self.cif)
        else:
            self.curcon.ready()
        return self.curcon

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.elock.release()

    def execute_db(self, cmd, get_ins_id=False):
        con = self.__take_kick()
        if con:
            cur = con.cursor()
            cur.execute(cmd)
            if cur.fetchone() and get_ins_id:
                cur.execute('SELECT LAST_INSERT_ID;')
                rt = cur.fetchone()[0]
            else:
                rt = cur.fetchone()
            cur.close()
            self.__take_kick(con)
            return rt
        raise RuntimeError('NO connection to take, with current finger=%s' % self.finger)

    def us_execute_db(self, sqlcmd):
        con = self.__take_kick()
        if con:
            cur = con.cursor()
            cur.execute('SET SQL_SAFE_UPDATES=0;')
            cur.execute(cmd)
            if cur.fetchone() and get_ins_id:
                cur.execute('SELECT LAST_INSERT_ID;')
                rt = cur.fetchone()[0]
            cur.execute('SET SQL_SAFE_UPDATES=0;')
            cur.close()
            self.__take_kick(con)
            return rt
        raise RuntimeError('NO connection to take')   

    def query_db(self, cmd, one=False):
        con = self.__take_kick()
        if con:
            cur = con.cursor()
            cur.execute(cmd)
            if one:
                rt = cur.fetchone()[0]
            else:
                rt = cur.fetchall()
            cur.close()
            self.__take_kick(con)
            return rt
        raise RuntimeError('NO connection to take')

    def do_sequence(self, sql_seq, ignore=False):
        con = self.__take_kick()
        cur = con.cursor()
        dbrt = []
        for result in cur.execute(sql_seq, multi=True):
            if result.with_rows:
                dbrt.append(result.fetchall())
            else:
                dbrt.append(result.rowcount)
        cur.close()
        self.__take_kick(con)
        return dbrt


class com_con2(object):
    w = 'pool'
    length = 10

    def __init__(self, poolname, server_args, length=0, **extargs):
        self.server = server_args if isinstance(server_args, dict) else server_args.D if isinstance(server_args, server_info) else None
        assert self.server
        self.pool = mcrp.MySQLConnectionPool(pool_name=poolname, pool_size=length or self.__class__.size, **self.server)
        if not self.pool:
            raise RuntimeError("Not Able to Create a connect pool for mysql!")
        self.lastcmd = ''
        self.curcon = None

    def __getitem__(self, cmds):
        if isinstance(cmds, (str, unicode)):
            operation = cmds.split(' ')[0]
            if operation.lower() in ('insert', 'update', 'delete'):
                return self.execute_db(cmds)
            else:
                rt = self.query_db(cmds)
                return rt[0] if rt and len(rt) == 1 else rt
        else:
            return self.execute_dbs(cmds)

    def shutdown(self):
        return True

    def reset(self, server_args):
        self.server = server_args
        self.pool.set_config(**server)

    def free(self):
        con = self.pool.get_connection()
        return con if con.is_connected() else None

    def release(self, con=None):
        return True

    def __enter__(self):
        self.elock.acquire()
        if self.curcon is None:
            logging.info('con for with is still None, create it!')
            self.curcon = self.pool.get_connection()
        return self.curcon

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.curcon.close()
        self.elock.release()

    def execute_db(self, cmd, get_ins_id=False):
        con = self.pool.get_connection()
        if con:
            cur = con.cursor()
            cur.execute(cmd)
            if cur.fetchone() and get_ins_id:
                cur.execute('SELECT LAST_INSERT_ID;')
                rt = cur.fetchone()[0]
            else:
                rt = 1
            cur.close()
            con.close()
            return rt
        raise RuntimeError('NO connection to take, with current finger=%s' % self.finger)

    def execute_dbs(self, cmds):
        cmdstr = ';'.join(cmds) if isinstance(cmds, (tuple, list)) else cmds
        con = self.pool.get_connection()
        cur = con.cursor(multi=True)
        dbrt = []
        for result in cur.execute(operation, multi=True):
            if result.with_rows:
                dbrt.append(result.fetchall())
            else:
                dbrt.append(result.rowcount)
        cur.close()
        con.close()
        return dbrt

    def us_execute_db(self, sqlcmd):
        con = self.pool.get_connection()
        if con:
            cur = con.cursor()
            cur.execute('SET SQL_SAFE_UPDATES=0;')
            cur.execute(cmd)
            if cur.fetchone() and get_ins_id:
                cur.execute('SELECT LAST_INSERT_ID;')
                rt = cur.fetchone()[0]
            cur.execute('SET SQL_SAFE_UPDATES=0;')
            cur.close()
            con.close()
            return rt
        raise RuntimeError('NO connection to take')     

    def query_db(self, cmd, one=False):
        con = self.pool.get_connection()
        if con:
            cur = con.cursor()
            cur.execute(cmd)
            if one:
                rt = cur.fetchone()[0]
            else:
                rt = cur.fetchall()
            cur.close()
            con.close()
            return rt
        raise RuntimeError('NO connection to take')


if __name__ == '__main__':
    server = server_info().D
    testpool = com_con('testing', server, length=3)
    # testdbc = mdb_mysql(server)
    # if testdbc.connect_db() == 1:
    #     print 'YES'
    # else:
    #     print 'NO'
    # print testdbc['show tables;']
    # testdbc.close()
