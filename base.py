#!/usr/bin/env python
#-*- coding:utf-8 -*-
import os
import log
import sys
import time
import yaml
import pymongo

from multiprocessing import Process


def config():
    """
    parse config.ini return dict.
    """
    return yaml.load(open('config/config.ini'))


class BaseClass(object):
    """
    model base class
    """

    def __init__(self):
        super(BaseClass, self).__init__()

        self.config = config()

        self.mgconn = pymongo.MongoClient(self.config.get("mongodb"))

        self.log = log
        self.log.set_logger(level=self.config.get("log_level"), when="D", limit=1)
        #Todo库名称
        #self.mgdb = self.mgconn.test
        self.mgdb = self.mgconn.msdk_stat

        self.hive = self.config.get("hive")
        self.hadoop = self.config.get("hadoop")
        self.logpath = self.config.get("log_path")

    @property
    def now(self):
        """
        return string current localtime
        """
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


class BaseProcess(BaseClass, Process):
    """
     Base Class of job multiprocessing
    """

    def __init__(self):
        super(BaseProcess, self).__init__()


if __name__ == "__main__":
    sys.exit(0)
