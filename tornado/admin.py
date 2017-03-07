#!/usr/bin/env python
#-*- coding:utf-8 -*-
import os
import yaml
import handler

import tornado.ioloop
import tornado.web

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
        (r"/", handler.Application)
    ], **settings)




def loop():
    """
    The entry function
    """
    conf = yaml.load(open('../config/config.ini'))
    router().listen(conf.get("listen"))
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    router().listen(8888)
    tornado.ioloop.IOLoop.current().start()
