#!/usr/bin/env python
"""try:
	raise ImportError
	from setuptools import setup
	from distutils.sysconfig import get_python_version
except ImportError:"""
from distutils.core import setup
import os

init_files = ('/etc/init.d/', ['smartproxyd'])
conf_files = ('/etc/lounge/', ['smartproxy.xml', 'smartproxy.tac', 'cacheable.json.example'])
check_files = ('/root/bin/', ['check-smartproxy.py'])
cron_files = ('/etc/cron.d/', ['check-smartproxy'])
cache_files = ('/var/lib/lounge/smartproxy', ['cache.dat'])

data_files = [init_files, conf_files, check_files, cron_files, cache_files]

py_modules = ["smartproxy.proxy", "smartproxy.fetcher", "smartproxy.reducer", "smartproxy.streaming", "smartproxy.changes", "smartproxy.lrucache"]

setup( version = '2.1.6',
	   name = 'lounge-smartproxy2',
	   author='meebo',
	   author_email='shaun@meebo-inc.com',
	   url='http://tilgovi.gthhub.com/couchdb-lounge',
	   data_files = data_files,
	   py_modules = py_modules)
