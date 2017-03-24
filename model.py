#!/usr/bin/env python
#-*- coding:utf-8 -*-
"""
model class
The datebase is used mongodb.
"""
import os
import re
import sys
import cmds
import time
import types
import base64
import hashlib
import pymongo
import datetime
import traceback
import subprocess


from libs import utils
from base import BaseClass
from bson.objectid import ObjectId


# singleton decorator
def singleton(cls, *args, **kw):
    instances = {}

    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return _singleton


class TaskInit(BaseClass):
    """
    task_libiary init, save tasks to mongo database
    """

    def __init__(self, ddate, tkey=None, tday=None):
        super(TaskInit, self).__init__()
        self.ddate = ddate
        self.tkey = tkey

        if tday is None:
            self.tday = self.ddate
        else:
            self.tday = tday

    def initTasks(self):
        """
        a concrete realization of tasks initialization
        """
        if self.tkey is None:
            num = TaskHistory(date=self.ddate).search(
                count=True, task_day=self.ddate)
            hour = int(time.strftime("%H"))
            # a hack, many times run init module
            if num > 20 and hour >= 1:
                self.log.info("Initialization has been completed")
                return True
            tlist = TaskLibrary().allTask()
        else:
            tlist = TaskLibrary().getByKey(self.tkey)
        if not tlist:
            self.log.debug("no tasks")
            return False

        ts = TaskHistory()
        for task in tlist:
            # status not 1, not init it.
            if int(task.get("status", 0)) != 1:
                continue
            task = self.__parseTask(task)
            if self.__checkInited(task.get("task_day"), task.get("task_key"), task.get("task_type")):
                continue
            ts.insert(task)

        self.log.info("init task finished")
        return True

    def insertHistory(self, task):
        """
        @deprecated
        """
        if not task:
            return False
        rs = self.mgdb.task_history.insert(task)
        self.log.info("init insert mongodb _id:%s" % rs)
        return rs

    def __parseTask(self, task):
        """
        parse task property
        """
        if not task:
            return False
        task['cmd'] = self.__replaceLog(
            self.__replaceDate(task.get('cmd'), self.tday))
        task['ctime'] = self.now
        task['status'] = 'waiting'
        task['task_day'] = self.ddate
        task['finish_time'] = '0'
        if task.get("rely"):
            if task.get("task_type") == 'crontab':
                task['rely'] = {task.get("rely"): 0}
            else:
                # remove both ends ","
                task['rely'] = {x.strip(): 0 for x in task.get(
                    "rely").strip(",").split(",")}

        # hour cycle task
        if task.get("cycle_type") == "hour":
            task['task_hour'] = time.strftime(
                '%M', time.localtime(time.time()))

        # remove no use field
        if '_id' in task:
            del task['_id']
        if 'isinit' in task:
            del task['isinit']

        return task

    def __replaceDate(self, hql, date):
        """
        <date>,<date-n> set specific date 
        """
        #%%escapa
        hql = hql.replace("<date>", date).replace('%', '%%')
        # gerp date-n
        #Re = re.compile(r'<date\s*([-+]\s*\d+)')
        Re = re.compile(r'<date\s*([-+]\s*\d+)\|?(\S*?\s*\S*?)>')
        l = Re.findall(hql)
        if not l:
            return hql

        l = map(lambda x: (int(x[0]), x[1]), l)
        for x in l:
            if x[1]:
                f = ''.join(
                    map(lambda c: '%' + c if re.match('^[A-Za-z]', c) else c, x[1]))
            else:
                f = '%Y%m%d'
            stamp = int(time.mktime(time.strptime(
                date, '%Y%m%d'))) + 86400 * x[0]

            match = Re.search(hql)
            if not match:
                continue

            # replace <date-n|[Ymd]> to specific time.
            sdate = time.strftime(f, time.localtime(stamp))
            hql = hql.replace(match.group(), str(sdate))

        return hql

    def __replaceLog(self, hql):
        """
        repalce shell or hql <logpath>, <hive>, <hadoop>
        """
        return hql.replace("<logpath>", self.logpath).replace("<hive>", self.hive).replace("<hadoop>", self.hadoop)

    def __checkInited(self, date=None, tkey=None, ttype=None):
        """
        checking the task has been already initialized
        """
        if ttype == 'crontab':
            return False
        if date is None:
            date = self.ddate

        for doc in TaskHistory().search(task_day=date, task_key=tkey):
            if doc.get("status") == "waiting":
                return True
        return False


class TaskLibrary(BaseClass):
    """
    service logic of mongo database collection task_library
    """

    def __init__(self):
        super(TaskLibrary, self).__init__()

    def allTask(self, isCron=False):
        """
        return all tasks, type is list
        arguments isCron is crontab
        """
        if isCron:
            m = {"task_type": "crontab"}
        else:
            m = {"task_type": {"$ne": "crontab"}}
        tlist = []
        for doc in self.mgdb.task_library.find(m):
            tlist.append(doc)
        return tlist

    def getByKey(self, taskKey):
        """
        get one task from task_library, return dict
        """
        tlist = []
        for doc in self.mgdb.task_library.find({"task_key": taskKey}):
            tlist.append(doc)
        return tlist

    def getModifyTask(self):
        """
        get current modifyed tasks
        """
        tlist = []
        for doc in self.mgdb.task_library.find({"isinit": u'1'}):
            tlist.append(doc)
        return tlist

    def updateByOid(self, oid, **kwargs):
        """
        update
        """
        if not oid:
            return False
        return self.mgdb.task_library.update({"_id": ObjectId(oid)}, {"$set": kwargs})

    def getFollows(self, tkey):
        """
        get downstream tasks.
        """
        flist = []
        for doc in self.mgdb.task_library.find({"rely": {"$regex": ',' + tkey + ','}}):
            flist.append(doc)

        return flist


class TaskHistory(BaseClass):
    """
    TaskHistory
    """

    def __init__(self, date=None):
        super(TaskHistory, self).__init__()
        self.date = date

    def checkRely(self, task):
        """
        check upstream is finished
        """
        if not isinstance(task, dict):
            return False
        keys = task.get("rely")
        #is empty or crontab, explain upstream is true
        if not keys or task.get("task_type") == "crontab":
            return True

        keyl = []
        for k, v in keys.items():
            keyl.append(k)

        date = task.get("task_day")
        if not date:
            date = self.date

        mkeys = [{"task_key": k} for k in keyl]
        tlist = {}
        for doc in self.mgdb.task_history.find({"$or": mkeys, "task_day": date}):
            tlist[doc.get("task_key")] = doc

        if not tlist or len(tlist) != len(mkeys):
            #when debug, always return true.
            if self.config.get("is_debug"):
                return True
            else:
                return False
        for c, d in tlist.iteritems():
            if d.get("status") != "finished":
                return False

        return True

    def insert(self, task):
        """
        insert fun
        """
        if not task:
            return False
        id = self.mgdb.task_history.insert(task)
        self.log.info("init insert mongodb _id:%s" % id)
        return id

    def getByDay(self, date=None, num=False):
        """
        get warting tasks or some failure less than three times
        """
        if date is None:
            date = self.date

        tdate = datetime.datetime.strptime(
            date, "%Y%m%d") - datetime.timedelta(180)
        ldate = tdate.strftime("%Y%m%d")
        ddict = {"$or": [{"task_day": {"$gt": ldate}, "status": "waiting"}, {
            "task_day": date, "status": "failure", "retry": {"$lte": 3}}]}
        if num:
            return self.mgdb.task_history.find(ddict).count()
        else:
            l = []
            for doc in self.mgdb.task_history.find(ddict).sort('level', pymongo.DESCENDING):
                l.append(doc)
            return l

    def getByOid(self, oid):
        """
        oid->ObjectId(oid)
        """
        if not oid:
            return None
        return self.mgdb.task_history.find_one({"_id": ObjectId(oid)})

    def cronWaitingList(self, date):
        """
        fetch task the type is crontab and not finished 
        """
        match = {"task_type": "crontab", "task_day": date, "status": "waiting"}
        l = []
        for doc in self.search(match):
            l.append(doc)
        return l

    def search(self, ddict=None, count=False, **kwargs):
        """
        search fun
        """
        if isinstance(ddict, dict):
            kwargs = ddict
        if not kwargs:
            return None

        if count:
            return self.mgdb.task_history.find(kwargs).count()
        else:
            return self.mgdb.task_history.find(kwargs).sort("level", pymongo.DESCENDING)

    def updateByOid(self, oid, ddict=None, **kwdict):
        """
        update by object id
        """
        if not oid:
            return False
        if ddict is None:
            if kwdict.get("finish_time") is None:
                kwdict['finish_time'] = self.now

            if kwdict.get("retry"):
                self.mgdb.task_history.update(
                    {"_id": ObjectId(oid)}, {"$inc": {"retry": 1}})
                del kwdict["retry"]
        else:
            kwdict = ddict
            if not isinstance(kwdict, dict):
                return False

        return self.mgdb.task_history.update({"_id": ObjectId(oid)}, {"$set": kwdict})


class TaskParse(BaseClass):
    """
    parse
    """

    def __init__(self, mq=None):
        super(TaskParse, self).__init__()
        self.mq = mq

    def runTask(self, d):
        if not d or not isinstance(d, dict):
            return False
        # lawful task_type
        lawful = {'hive2mysql': 1, 'hive': 1,
                  'shell': 1, 'mysql': 1, 'crontab': 1}
        if not d.get("task_type") in lawful:
            self.log.info("task type error, task:%s" % str(d))
            return False

        # check log path
        date = d.get('task_day')
        self.mkdirLog(date)

        # check the task is finished
        dd = TaskHistory().getByOid(d.get("_id"))
        if not (dd.get("status") == "waiting" or (dd.get("status") == "failure" and dd.get("retry") <= 3)):
            self.log.info("task is not waiting status, task:%s" % str(dd))
            return True

        r = self.__runObj(d.get("task_type"), d.get("_id")).run(d)
        if not r:
            title = "Query task '%s' Failure" % d.get('task_key')
            body = "Task key: %s @ %s Failure, please check the log for details."
            body = body % (d.get('task_key'), date)
            utils.sendmail(self.config.get("maid_send"),
                           d.get('author').split(","), title, body)
        return True

    def __runObj(self, type, id):
        TaskHistory().updateByOid(
            id, {"start_time": self.now, "status": "running"})
        if type == "hive":
            return cmds.HiveCls(self.mq)
        if type == "mysql":
            return cmds.MysqlCls(self.mq)
        if type == "hive2mysql":
            return cmds.Hive2mysqlCls(self.mq)

        return cmds.ShellCls(self.mq)

    def mkdirLog(self, date):
        """
        make dir
        """
        path = os.path.join(self.logpath, date)
        if os.path.exists(path):
            return True
        os.makedirs(path)


class TaskRerun(BaseClass):
    """
    rerun
    """

    def __init__(self):
        super(TaskRerun, self).__init__()

    def do(self):
        """
        
        """
        hour = int(time.strftime("%H", time.localtime()))
        if hour <= 6 or hour >= 22:
            self.log.error("The current time process suspended")
            return False
        for doc in self.mgdb.task_rerun.find({"status": 1}):
            self.mgdb.task_rerun.update({"_id": ObjectId(doc.get("_id"))}, {
                                        "$set": {"status": 0}})
            self.__rerun(doc)
            self.log.info("rerun task:%s, task_day:%s" %
                          (doc.get("task_key"), doc.get("task_day")))

        return True

    def __rerun(self, d):
        """
       task initialization
        """
        if not d:
            return False
        ddate = time.strftime(
            "%Y%m%d", time.localtime(int(time.time()) - 86400))

        tdate = d.get("task_day", ddate)
        self.__upFailure(d.get("task_key"), tdate)
        
        obj = TaskInit(tdate, d.get("task_key"), tdate)
        obj.initTasks()

        if int(d.get("follow")) == 1:
            self.__initTask(d.get("task_key"), tdate)
        return True

    def __initTask(self, tkey, tday):
        """
        initialization downstream tasks by recursion
        """
        inited = {}
        for task in TaskLibrary().getFollows(tkey):
            self.log.info("recursion rerun task:%s, task_day:%s" %
                          (task.get("task_key"), tday))
            if task.get("task_key") in inited:
                continue
            #mark finished task stats bad, avoid affect check upstream task status.
            self.__upFailure(task.get("task_key"), tday)
            obj = TaskInit(tday, task.get("task_key"), tday)
            obj.initTasks()
            self.__initTask(task.get("task_key"), tday)
            inited[task.get("task_key")] = 1

        return True

    def __upFailure(self, tkey, tday):
        """
        update status bad
        """
        return self.mgdb.task_history.update_many({"task_key": tkey, 'task_day': tday, 'status': "finished"}, {"$set": {"status": "bad"}})



if __name__ == "__main__":
    sys.exit(0)

