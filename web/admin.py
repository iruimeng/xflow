#!/usr/bin/env python
#-*- coding:utf-8 -*-
from __future__ import absolute_import

import os
import sys
import yaml
import tornado.ioloop
import tornado.web

from . import history
from . import task





#CURRENT_PATH = os.path.dirname(__file__)


settings = {
    'debug': True,
    'gzip': True,
    'autoescape': None,
    'xsrf_cookies': False,
    'cookie_secret': '123456',
    'static_path' : os.path.join(os.path.dirname(__file__), "../static"),
    'template_path': os.path.join(os.path.dirname(__file__), "../static/templates")
}

def router():
    """
    router config
    """
    return tornado.web.Application([
        (r"/", task.ListController),
        (r"/add", task.AddController),
        ('r"/history', history.ListController),
    ], **settings)




def loop():
    """
    The entry function
    """
    conf = yaml.load(open(os.path.join(os.path.dirname(__file__), '../config/config.ini')))
    router().listen(conf.get("listen"))
    tornado.ioloop.IOLoop.current().start()

"""
if __name__ == "__main__":
    loop()
"""