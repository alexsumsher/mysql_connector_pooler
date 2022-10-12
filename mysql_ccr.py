# -*- coding:utf-8
"""
MySQL Database Connection.
change to fit for poolo class.
last update: 20171229.
update: com_con,增加flexiable模式的pool

20180103: com_con, __enter__ with elock(thread.Lock) for thread safe
201803：new arg for execute_db: get_ins_id(false), weather return last insert id
==>update: 20210830: [get_item as query with new mysql connection and close soon]
==>update: 20220226: 真实的完成了压力测试，基本上算是靠谱可用了。
==>update: 20220512: 进一步的强化并发稳定性。多线程1000并发0-1s随机时间。7con。flexible
==>update: 20220727: 简单增加readonly模式。
"""
import mysql.connector as mcr
import logging
import time
import random
#   for com_con
from threading import Lock
#logging.basicConfig(level=logging.DEBUG, format='(%(funcName)-10s) %(message)s')


class server_info:
    server_info = {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'sqluser',
        'password': 'sqlpwd',
        'db': 'sqldb',
        'charset': 'utf8',
        'connection_timeout': 3,
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

"""
sql_checker:
"""


class com_con(object):
    wait_con_time = 5
    length = 10
    dead_len = 30
    recover_time = 60
    # 120s not use pool, connections are disconnected from server
    # after testing: 953s is ok...// 1346s ok// 2047s ok
    sleeptime = 240
    #   w is a mark for pool
    w = 'pool'
    MODE_FLEXIBLE = 0
    MODE_LIST = 1
    sql_checker = None

    #   -1: not inited; 1: working; 0:shutdown
    def __init__(self, poolname, con_info, length=0, atonce=True, flexible=False, debug=False, readonly=False, **kwargs):
        # if flexible, work with lenth and dead_length, and self.c->last time work mode, if add new con is set to 1, if kick set to -1: 
        # when take: if overlen create new con, if over deadlen error
        # when kick: if finger > len and last time is kick, will not append to conlist for reuse, just remove it(on the other hand, if the last time action is still create new con, which means the pool may still works under busy mode)
        
        do_fill = True
        self.rmode = readonly
        if flexible:
            # starts with length==1
            # length as dead_length
            self.length = 1
            self.dead_len = length
            self.__take_kick = self.__take_kick_2
            self.c = 0
            self._mode = 0
            do_fill = False
        else:
            self.length = length or self.__class__.length
            self.dead_len = 0
            self.__take_kick = self.__take_kick_1
            self._mode = 1
        self.cif = con_info if isinstance(con_info, dict) else con_info.D if isinstance(con_info, server_info) else None
        if self.cif is None:
            raise RuntimeError("NO connection info!")
        self.debug = debug
        if self.debug:
            logging.info("COM_CON[%s]: debug on!" % poolname)
        self.name = poolname
        self.finger = 0
        self.aqrs = 0
        self.ilock = Lock()
        self.elock = Lock()
        self.status = -1
        self.conlist = []
        self.staticlist = []
        self.curcon = None
        self.recover_time = self.__class__.recover_time
        # too long no using
        #self.sleeptime = kwargs.get('sleeptime', self.__class__.sleeptime)
        #self.lasttime = time.time()
        self.w = 'pool'
        if not atonce:
            return
        self.ilock.acquire()
        try:
            self.__inilist(do_fill)
        finally:
            self.ilock.release()
    """
    def __getitem__(self, sqlcmds):
        ix = sqlcmds.find(';')
        if ix > 0 and len(sqlcmds) > ix + 1:
            return self.do_sequence(sqlcmds)
        else:
            if sqlcmds[:sqlcmds.find(' ')].lower() in ('select', 'show', 'desc'):
                rt = self.query_db(sqlcmds)
                return rt[0] if rt and len(rt) == 1 else rt
            else:
                return self.execute_db(sqlcmds)
    """
    def __getitem__(self, sqlcmd, *args):
        # only for select, no checker
        # if checker:
        # if not sqlcmd.startwith('select'):...
        # args: controller:
        # args[0]: one row?
        # args[1]: single column?
        if not sqlcmd.upper().startswith('SELECT'):
            return False
        # sql_checker:
        # if not self.sql_checker(sqlcmd):
        #   return False
        con = mcr.connect(**self.cif)
        cur = con.cursor()
        try:
            cur.execute(sqlcmd)
            result = cur.fetchall()
            cur.close()
        except mcr.Error as err:
            #logging.error("sql error: %s: %s" % (err.errno, err.msg))
            logging.error("sqlcmd error: %s" % sqlcmd)
            return False
        finally:
            con.close()
        return result

    def __testing(self):
        try:
            con = mcr.connect(**self.cif)
            logging.debug(con.is_connected())
        except:
            logging.debug(str(self.cif))
            return False
        return con

    def __takecon(self):
        #t = time.time()
        con = self.conlist.pop(0)
        try:
            con.ping(reconnect=True, delay=0)
            #con.ping(reconnect=False, attemps=1, delay=0) #it works well when local connection, maybe the connect delay likes 0; and mysql5.5+
        except:
            logging.error("CON: %s :this con is dead with ping!" % con.connection_id)
            self.staticlist.remove(con)
            con = mcr.connect(**self.cif)
            con.mark = 0
            logging.debug("new con: %s created!" % con.connection_id)
            self.staticlist.append(con)
        #self.lasttime = t
        return con

    # for flexable
    def __takecon2(self):
        try:
            con = self.conlist.pop(0)
            #print("con from takencon2")
        except IndexError:
            con = None
        if con:
            if con.is_connected():
                return con
            else:
                #print("con not connected, shutdown!")
                self.staticlist.remove(con)
                con.shutdown()
        for i in range(len(self.conlist)):
            con = self.conlist.pop(0)
            if not con.is_connected():
                logging.error("CON: %s :this con is dead with ping!" % con.connection_id)
                self.staticlist.remove(con)
                del con
            else:
                #print("con of idx: %d" % i)
                return con
        #print("all con clear!")
        # if no connection available
        try:
            con = mcr.connect(**self.cif)
            logging.debug("new con: %s created!" % con.connection_id)
            self.staticlist.append(con)
            self.length = len(self.staticlist)
            #self.lasttime = time.time()
        except:
            con = None
            logging.error("NOT ABLE to Create a new connection!")
        return con

    def __inilist(self, fill=True):
        if self.status > 0:
            if self.length > len(self.staticlist):
                for x in range(self.length - len(self.staticlist)):
                    con = mcr.connect(**self.cif)
                    self.staticlist.append(con)
                    self.conlist.append(con)
            return
        if self.status == 0:
            if len(self.conlist) > 0:
                for i in range(len(self.conlist)):
                    self.conlist.pop()
            if len(self.staticlist) > 0:
                for i in range(len(self.staticlist)):
                    self.staticlist.pop().close()
        tcon = self.__testing()
        if tcon is False:
            logging.error("cannot connect to mysql server!")
            return -1
        self.staticlist = [None] * self.length
        tcon.mark = 0
        self.staticlist[0] = tcon
        self.conlist.append(tcon)
        if fill:
            for i in range(1, self.length):
                con = mcr.connect(**self.cif)
                con.mark = 0
                self.conlist.append(con)
                self.staticlist[i] = con
                time.sleep(0.05)
        logging.debug("INITIAL POOL DONE!")
        self.status = 1
        return self.status

    def __fix_cons(self, con=None):
        try:
            cur = None
            checkcon = mcr.connect(**self.cif)
            cur = checkcon.cursor()
            cur.execute("show processlist")
        except:
            logging.warning("fix con failure!")
            if cur:
                cur.close()
                checkcon.close()
            return False
        dbrt = cur.fetchall()
        if not dbrt:
            return False
        for con_stat in dbrt:
            # con_stat: id:0, User:1, Host:2, db: 3, Command:4, Time:5
            if con_stat[3] == 'Query' and con_stat[5] > 30:
                #long query
                cur.execute("kill QUERY %s" % con_stat[0])
                logging.info("reset a long query connect: %s" % con_stat[0])
                for _ in range(len(self.conlist)):
                    if _.connection_id == con_stat[0]:
                        self.conlist.remove(_)
                        self.staticlist.remove(_)
                        break
        cur.close()
        checkcon.close()
        return True

    def resetcon(self, con, autoappend=True):
        logging.warning("a con to be reset！")
        self.staticlist.remove(con)
        try:
            con = mcr.connect(**self.cif)
            if autoappend:
                self.staticlist.append(con)
        except:
            logging.error("Not able to create a new connection!")
            return None
        return con

    def __batch_recovery(self):
        # if lite mode: connections less than 4, force release first and return
        logging.warning("on __batch_recovery!")
        self.conlist.clear()
        #for _ in range(len(self.conlist)):
        #    self.conlist.pop()
        for _ in range(len(self.staticlist)):
            con = self.staticlist.pop()
            if not con.is_connected():
                logging.warning("a con is not connectable, remove!")
                del con
                continue
            if con.unread_results:
                con.get_rows()
            con.mark = 0
            self.conlist.push(con)
            logging.info("recover a con!")
        for _ in range(self.length - len(self.staticlist)):
            con = mcr.connect(**self.cif)
            self.conlist.append(con)
            self.staticlist.append(con)
            time.sleep(0.05)
        self.finger = 0
        self.length = len(self.conlist)
        return self.finger

    def __str__(self):
        return 'pool status: %d\tfinger: %d\t; usage: %d/%d' % (self.status, self.finger, len(self.conlist), len(self.staticlist))

    __repr__ = __str__

    def __take_kick_2(self, con=None):
        # works for flexable mode
        # 10个并发threading-产生4个con
        # 100个并发threading-产生6个con
        # 待升级：con的回收机制:
        # 1) 内部-定时测试-回收
        # 2）内部-定量（使用量）测试-回收
        # 3）外部-定时回收
        if con:
            if self.debug:
                logging.debug('release con: finger:%d\t; usage: %d/%d;' % (self.finger, len(self.conlist), len(self.staticlist)))
            if self.finger > self.dead_len:
                con.close()
                self.staticlist.remove(con)
                del con
            else:
                self.conlist.append(con)
            self.finger -= 1
            return self.finger
        if self.debug:
            logging.debug('con_pool=>status: %d\tfinger: %d\t; usage: %d/%d' % (self.status, self.finger, len(self.conlist), len(self.staticlist)))
        # currently no use
        if self.aqrs == 1:
            time.sleep(3)
            if self.ilock.locked():
                self.ilock.release()
            self.aqrs = 0
            return None

        if self.status <= 0:
            try:
                rt = self.__inilist(False)
                if rt != 1:
                    self.aqrs = 0
                    return None
                self.length = 1
            except:
                return None
        for i in range(10):
            if self.ilock.acquire(timeout=3):
                break
            time.sleep(random.random())
            if i == 9:
                logging.error("begin with kicktake: ilock not get!")
                return None
        con = self.__takecon2()
        self.ilock.release()
        if not con:
            return None
        self.finger += 1
        # mark time for analyzation or sysgard
        con.tmark = time.time()
        return con

    def __take_kick_1(self, con=None):
        if con:
            self.conlist.append(con)
            #con.mark = 0
            self.finger -= 1
            #logging.debug("release con: %s" % str(self))
            return self.finger
        #if self.debug:
        #    logging.debug('conpool=>status: %s\tfinger: %s\t; usage: %s/%s' % (self.status, self.finger, len(self.conlist), len(self.staticlist)))
        if not self.ilock.acquire(timeout=5):
            logging.error("begin with kicktake: ilock not get!")
            return None
        # re-initial or recover are both auto call by take_kick, with ilock on
        # test for re initial
        if self.status <= 0:
            try:
                self.__inilist()
            except:
                logging.warning("ini list error!")
            finally:
                self.ilock.release()
            if self.status != 1:
                return None
            self.finger = 0
        # test for recovering
        # [错误的设计：使用率超标意味着只是没有可用的，并非需要recovery]
        if self.finger >= self.length:
            if self.status!=1 and self.ilock.locked():
                try:
                    logging.warning("COM_CON status != 1 and recovery!")
                    self.__batch_recovery()
                except:
                    logging.error("COM_CON recovery failure!")
                    self.status = 0
                    self.ilock.release()
                    return None
            #wait wait_con_time for available con
            #self.ilock.acquire(timeout=1)
            if not self.ilock.locked():
                logging.error("not able to get ilock for waiting.")
                return None
            logging.warning("COM_CON: waiting 5s for available con.")
            for i in range(int(self.wait_con_time/0.1)):
                time.sleep(0.1)
                if len(self.conlist)==0:
                    continue
                else:
                    con = self.__takecon()
                    break
            # length==2 会导致出错
            #if self.length>10 and self.finger>=self.length-1:
            if self.finger>self.length:
                if not self.ilock.locked():
                    logging.warning("try to fix cons but not get lock.")
                    return None
                logging.warning("COM_CON: fix con.")
                if self.__fix_cons() is False:
                    self.ilock.release()
                    return None
        else:
            # 正常流程
            con = self.__takecon()
        self.finger += 1
        if self.ilock.locked():
            self.ilock.release()
        return con

    def __enter__(self):
        self.elock.acquire(timeout=5)
        try:
            if self.curcon is None:
                logging.info('con for with is still None, create it!')
                self.curcon = mcr.connect(**self.cif)
                self.curcon.ready = 0
            else:
                self.curcon.ping(reconnect=True, attempts=1, delay=0)
        except mcr.InterfaceError:
            logging.warning("__enter__ connection is down and not able to reconnect")
            self.curcon = None
            self.elock.release()
        except:
            self.curcon = None
            self.elock.release()
            #raise RuntimeError("ENTER CON Failure!")
        return self.curcon

    def __exit__(self, exc_type, exc_val, exc_tb):
        # print(exc_val)
        self.elock.release()
        self.curcon.ready = 1
        return True

    def shutdown(self):
        self.ilock.acquire(False)
        logging.warning("shutting down com_con instance!")
        for _ in range(len(self.staticlist)):
            con = self.staticlist.pop()
            logging.info(f"con {con.connection_id} to shutdown.")
            con.shutdown()
            time.sleep(0.05)
        if self.curcon and self.curcon.ready and self.curcon>=0:
            logging(f"con {self.curcon.connection_id} to shutdown.")
            self.curcon.shutdown()
        self.status = 0
        self.ilock.release()

    def free(self):
        con = self.__take_kick()
        if con:
            con.mark = int(time.time())
            return con
        else:
            return None

    def release(self, con=None):
        if con:
            if con.unread_result:
                logging.debug("release a con with unread_result")
                con.get_rows()
            con.mark = 0
            return self.__take_kick(con)
        else:
            return self.__batch_recovery()

    def execute_db(self, cmd, get_ins_id=False):
        # waiting for sql_checker:
        if self.rmode:
            logging.warning("read only mode!")
            return False
        con = self.__take_kick()
        if con is None:
            logging.warning("no connection taken!")
            return None
        if self.debug:
            logging.debug(cmd)
        rt = None
        try:
            cur = con.cursor()
            cur.execute(cmd)
            if get_ins_id:
                rt = cur.lastrowid
            else:
                rt = cur.rowcount
            cur.close()
        except Exception as e:
            logging.error(e)
            logging.error("Failure Execute: " + cmd)
            if not con.is_connected():
                logging.error("Execute db Failure for [con is not connected!]")
                #self.staticlist.remove(con)
                con = self.resetcon(con)
                if con is None:
                    return False
            rt = False
        self.__take_kick(con)
        return rt

    def us_execute_db(self, sqlcmd):
        if self.rmode:
            logging.warning("read only mode!")
            return False
        # sql_checker:
        con = self.__take_kick()
        if con is None:
            logging.warning("no connection taken!")
            return None
        logging.debug(sqlcmd)
        rt = None
        cur = con.cursor()
        #cmd = ';'.join(('SET SQL_SAFE_UPDATES=0', sqlcmd, ''))
        try:
            #cur.execute(cmd, multi=True)
            cur.execute('SET SQL_SAFE_UPDATES=0;')
            cur.execute(sqlcmd)
            rt = cur.rowcount   # rt could be 0 as no delete
        except:
            rt = False
        finally:
            cur.execute('SET SQL_SAFE_UPDATES=1;')
        cur.close()
        self.__take_kick(con)
        return rt

    def query_db(self, cmd, one=False, single=False, raw=False):
        # sql_checker:
        # @one: is one row?
        # @single: is single column per row?
        # @raw: no auto convert, just bytearray...
        # raw: list of list of bytearray like: (bytearray(b'159'), bytearray(b'170'), bytearray(b'0'), bytearray(b'0'), bytearray(b'1'), bytearray(b'4'), bytearray(b'0')
        con = self.__take_kick()
        rt = None
        if con is None:
            logging.warning("no connection taken!")
            return None
        if self.debug:
            logging.debug(cmd)
        try:
            cur = con.cursor(raw=raw)
            cur.execute(cmd)
            rt = cur.fetchall()
        except mcr.Error as err:
            logging.error("sql error: %s: %s" % (err.errno, err.msg))
            logging.error(cmd)
            if not con.is_connected():
                con = self.resetcon(con)
                if con is None:
                    return False
            elif cur.rowcount>0:
                cur.fetchall()
                cur.close()
            self.__take_kick(con)
            rt = False
        else:
            #if con.unread_result:
            #    con.get_rows()
            cur.close()
            self.__take_kick(con)
        if not rt:
            return rt
        if one:
            if single:
                return rt[0][0]
            rt = rt[0]
        if single:
            return [_[0] for _ in rt]
        return rt

    def do_sequence(self, sql_seq, ignore=True, results=False):
        # readonly 不支持序列
        if self.rmode:
            logging.warning("read only mode!")
            return False
        # execute(,mulit=True)在碰到中途问题语句时执行不完整
        con = self.__take_kick()
        cur = con.cursor()
        if results:
            dbrt_count = []
        else:
            dbrt_count = 0
        sql_seq = sql_seq if isinstance(sql_seq, list) else sql_seq.split(";")
        i = 1
        #cur.execute(sql_seq, multi=True)
        for sql in sql_seq:
            try:
                result = cur.execute(sql)
                if result and result.with_rows:
                    # could be no change, result none
                    result.fetchall()
                if results:
                    dbrt_count.append(result)
                else:
                    dbrt_count += 1
                i += 1
            except Exception as E:
                logging.error("error with: %s in sequence number: %d" % (E,i))
                if not ignore:
                    break
                i += 1
        cur.close()
        self.__take_kick(con)
        return dbrt_count
    # if transaction... use with and con autocommit switch

class rwcon(com_con):

    def __init__(self, wcon_conf, rcon_conf, rlen, ratonce=False):
        self.wconf = wcon_conf
        super(rwcon, self).__init__('rcons', rcon_conf, length=rlen, atonce=ratonce, flexible=False)

    def execute_db(self, cmd, get_ins_id=False):
        con = mcr.connect(**self.wcon_conf)
        if con:
            cur = con.cursor()
            logging.debug(cmd)
            rlt = cur.execute(cmd)
            if get_ins_id:
                rt = cur.lastrowid
            else:
                rt = cur.rowcount
            cur.close()
            con.close()
            return rt
        raise RuntimeError('NO connection to write server!')

    def us_execute_db(self, sqlcmd):
        con = mcr.connect(**self.wcon_conf)
        if con:
            cur = con.cursor()
            cmd = ';'.join(('SET SQL_SAFE_UPDATES=0', sqlcmd, 'SET SQL_SAFE_UPDATES=1'))
            cur.execute(cmd)
            cur.close()
            con.close()
            return 1
        raise RuntimeError('NO connection to write server!')


if __name__ == '__main__':
    #server = server_info().D
    server = {'host': 'localhost', 'user': 'root', 'db':'x', 'password': 'x'}
    print(server)
    testpool = com_con('testing', server, length=3)
    print(testpool)
    dbrt = testpool['select * from mysql.user']
    print(dbrt)
    # testdbc = mdb_mysql(server)
    # if testdbc.connect_db() == 1:
    #     print 'YES'
    # else:
    #     print 'NO'
    # print testdbc['show tables;']
    # testdbc.close()
