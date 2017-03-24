#!/usr/bin/env python
#-*- coding:utf-8 -*-
from distutils.core import setup
setup(
    name='xflow', 
    version='1.0', 
    py_modules=['xflow'],

    install_requires=[
        'tornado>=4.4.0'
    ],
    author='MT',
    author_email="vipmengrui@gmail.com"
)