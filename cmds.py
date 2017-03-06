#!/usr/bin/env python
#-*- coding:utf-8 -*-
"""
execute shell scripy, shell cmd，hive sql eg。

"""
import os
import log
import sys
import time
import json

import libs
import model
import urllib
import requests
#import urllib2
import subprocess

#from poster.encode import multipart_encode, MultipartParam
#from poster.streaminghttp import register_openers
from abc import ABCMeta, abstractmethod
from base import BaseClass
from multiprocessing import Process, Lock, Queue


class interface(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def run(self):
        pass


class ShellCls(interface, BaseClass):

    def __init__(self, mq):
        super(ShellCls, self).__init__()
        # message queue
        self.mq = mq

    def run(self, d):
        """
        entrance
        """
        if d is None:
            return False
        cmd = d.get("cmd")
        outlog, errlog = self.logFile(
            d.get('task_key'), d.get("task_day"), d.get("task_hour"))

        r = self.cmdRun(cmd, outlog, errlog)
        return self.taskGc(r, d, "shell")

    def logFile(self, key, date, hour=None):
        """
        :return log path
        """
        if hour is None or hour == "":
            r = os.path.join(self.logpath, date, key)
        else:
            r = os.path.join(self.logpath, date, key + "_" + str(hour))

        return (r + ".txt", r + ".log")

    def cmdRun(self, cmd, out=None, err=None, timeout=10800):
        """
        execute shell cmd.
        use subprocess poll
        """
        if out is None:
            out = subprocess.PIPE
        if err is None:
            err = out
        # when log exist，rename it.
        if os.path.exists(out):
            os.rename(out, out + time.strftime("%H", time.localtime()))

        outfd = open(out, 'w')
        errfd = open(err, 'w')
        sp = subprocess.Popen(
            cmd, bufsize=2048, stdout=outfd, stderr=errfd, shell=True)

        currTime = time.time()

        while sp.poll() is None:
            time.sleep(5)
            if int(time.time() - currTime) > timeout:
                # sp.kill()
                self.log.warning("Timeout 10800: %s" % cmd)
                return None
        sp.communicate()
        outfd.close()
        errfd.close()
        return sp.returncode

    def taskGc(self, r, d, type=None):
        """
        When a tasf finised, update status. and put current task key in a queue.
        used wake up or notify next task run
        """
        mt = model.TaskHistory()

        now = BaseClass().now
        if r == 0 or r is True:
            mt.updateByOid(d.get("_id"), status="finished",
                           finish_time=now, retry=1)
            if self.mq:
                self.mq.put(d)
            return True
        elif r < 0:
            msg = '%s was terminated by signal %s' % (
                d.get('task_key'), str(r))
            self.log.error(msg)

            if int(d.get("retry", 1)) <= 1:
                title = "Scheduler task '%s' may be killed" % d.get('task_key')
                libs.utils.sendmail(self.c['emailer'], d.get('author').split(
                    ","), title, 'Task %s failure at %s. %s' % (d.get('task_key'), now, msg))
        else:
            # task running failure
            self.log.error("task running failure, task:%s" % str(d))
            if int(d.get("retry", 1)) <= 1:
                title = "Scheduler task '%s' Failure" % d.get('task_key')
                libs.utils.sendmail('mengrui-g', d.get('author').split(","),
                                    title, 'Task %s failure at %s' % (d.get('task_key'), now))
        # If the setting retry_max value, didn't set the rertry_max 3,
        # determine the current number of executions to update as waiting.Let
        # it be executed multiple times.
        if int(d.get("retry", 1)) < int(d.get("retry_max", 3)):
            mt.updateByOid(d.get("_id"), status="waiting",
                           finish_time=now, retry=1)
            return True
        mt.updateByOid(d.get("_id"), status="failure",
                       finish_time=now, retry=1)
        return False


class HiveCls(ShellCls):
    """
    hive sql
    """

    def __init__(self, mq):
        super(HiveCls, self).__init__(mq)

    def run(self, d):
        outlog, errlog = self.logFile(d.get('task_key'), d.get('task_day'))
        rs = self.cmdRun(self.pcmd(d.get('cmd')), outlog, errlog)
        return self.taskGc(rs, d, 'hive')

    def pcmd(self, cmd):
        return "%s -e \"%s\"" % (self.hive, cmd)


class MysqlCls(HiveCls):
    """
    mysql2mysql
    """

    db = None

    def __init__(self, mq):
        super(MysqlCls, self).__init__(mq)

    def conn(self, db):
        """
        mysql connection
        """
        c = self.config.get("mysqldb")
        if c is None:
            self.log.error("config/__init__.py mysql config error, db:%s" % db)
            return False
        try:
            if self.db == None:
                self.db = libs.torndb.Connection(
                    c['host'], c['database'], c['user'], c['pwd'])
            return self.db
        except Exception, e:
            self.log.debug(traceback.format_exc())
            try:
                self.db = libs.torndb.Connection(
                    c['host'], c['database'], c['user'], c['passwd'])
                return self.db
            except Exception, e:
                self.log.error(traceback.format_exc())

    def run(self, d):

        ThObj = model.TaskHistory()
        ThObj.updateByOid(d.get("_id"), status="running", start_time=self.now)

        sql = d.get('hql').replace('"', '\\"').replace('%', '%%')

        db = self.conn()

        if db and db.execute(sql):
            ThObj.updateByOid(d.get("_id"), status="finished",
                              finish_time=self.now)
            rs = True
        else:
            ThObj.updateByOid(d.get("_id"), status="failed")
            rs = False
        return self.taskGc(rs, d, 'mysql')


class Hive2mysqlCls(HiveCls):
    """
    exec hql and load log 2 mysql
    """

    def __init__(self, mq=None):
        super(Hive2mysqlCls, self).__init__(mq)

    def run(self, d):
        pass

    def __table(self, table):
        """
        get table name
        """
        if not table:
            return False
        return table.strip().lower()

    def __loadFile2Mysql(self, d, log):
        """
       
        """
        #field = self.__parseOut(d.get('out_field'))
        table = self.__table(d.get('out_table'))

        if table == False:
            self.log.error("task out_values set error, task:%s" % str(d))
            return False

        param = {}
        param['db'] = d.get("out_db")
        param['table'] = table
        param['field'] = d.get("out_field")
        self.__request(log, param)
        return True

    def __parseOut(self, outval):
        """
        paser table conifg
        name:char|age:int
        """
        field = outval.strip("|").split("|")

        return map(lambda x: x.strip(":")[0], field)

    def __request(self, log, params, url):
        """
        Post to submit the log to the HTTP interface, 
        complete communication or file synchronization across systems
        """

        if url is None:
            self.log.error("url args is None")
            return False

        files = {'file': open(log, 'rb')}
        r = requests.post(url, files=files, data=params)

        try:
            d = json.loads(r.text,  encoding="utf-8")
            if d.get('err', 1) == 0:
                return True
            self.log.warning(r.text)
            return False
        except Exception, e:
            self.log.error(str(e))
            return False


class Hdfs2MysqlCls(Hive2mysqlCls):
    pass


if __name__ == "__main__":
    sys.exit(0)
