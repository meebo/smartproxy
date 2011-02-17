#!/usr/bin/python

import os
import os.path
import sys

failed = False
for f in os.listdir('test'):
  if f.endswith('_test.py'):
    print "Running", f
    os.chdir('test')
    try:
      ret = os.system("python " + f)
      if os.WEXITSTATUS(ret):
        failed = True
    finally:
      os.chdir('..')

# stop the makefile if any tests failed
sys.exit(failed and 1 or 0)

# vi: noexpandtab ts=2 sw=2
