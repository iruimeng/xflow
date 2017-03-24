#!/usr/bin/env python
#-*- coding:utf-8 -*-

import tornado.web

class ListController(tornado.web.RequestHandler):
    """
    tasks execution records，include task content, start time and spend time.
    """
    def get(self):
        pass