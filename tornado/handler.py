#!/usr/bin/env python
#-*- coding:utf-8 -*-

import tornado.web

class Application(tornado.web.RequestHandler):
    """
    task manager web application
    """
    def get(self):
        self.render("tasks.html")
        #self.write("Hello, world")
