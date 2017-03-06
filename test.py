#!/usr/bin/env python
#-*- coding:utf-8 -*-

import unittest
import base


class mytest(unittest.TestCase):
    """
    unit test
    """

    def setUp(self):
        pass

    def runTest(self):
        o = base.BaseProcess()
        print o.hive

    def test_config(self):
        """
        print config
        """
        print base.BaseClass().config


if __name__ == "__main__":
    unittest.main()
