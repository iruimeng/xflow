#!/usr/bin/env python
#-*- coding:utf-8 -*-

import tornado.web

class ListController(tornado.web.RequestHandler):
    """
    task manager web application
    """
    def get(self):
        self.render("tasks.html")



class AddController(tornado.web.RequestHandler):
    """
    task crdu
    """

    def get(self):
        d = {
            'sidebar':'add'
        }
        sidebar = "add"

        self.render("addt.html", sidebar=sidebar)