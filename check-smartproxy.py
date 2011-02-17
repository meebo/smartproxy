#!/usr/bin/env python
import commands
import os
import smtplib
import socket
import stat
import string
import sys
import time
import traceback
import urllib

from email.Utils import COMMASPACE
from lounge.prefs import Prefs

EMAIL_NOTIFICATION_LIST = ['kevin@example.com', 'vijay@example.com', 'shaun@example.com']
OPS_NOTIFICATION = 'ops@example.com'

# restart (if necessary) and send email alert:
def restartAndSendEmailAlert(restart, subject, msg, alertops = False):
	if restart:
		restart_msg = commands.getoutput("/etc/init.d/smartproxyd restart")
		# append restart msg:
		msg += "\n\nRestarting smartproxyd: %s\n\n" % restart_msg

	sender = 'root@%s' % socket.gethostname()
	receivers = EMAIL_NOTIFICATION_LIST
	# add ops to the receivers if necessary:
	if alertops:
		receivers.append(OPS_NOTIFICATION)

	server= smtplib.SMTP('localhost')

	# if we restarted, wait for a few seconds before appending the log, so that smartproxyd has time to start:
	if restart:
		time.sleep(5)

	# append the last 30 lines of the smartproxyd.log to the msg:
	log = commands.getoutput("tail -n 30 /var/log/lounge/smartproxyd.log")
	msg += "\n\nsmartproxyd.log (last 30 lines):\n" + log
	
	server.sendmail(sender, receivers,
                    ('From: %s\n'
                     + 'To: %s\n'
                     + 'Subject: %s: %s\n\n'
                     +  '%s') % (sender, COMMASPACE.join(receivers), socket.gethostname(), subject, msg))
	server.quit()


# first check the status based on whether the pid file is present or not:
if os.system("/etc/init.d/smartproxyd status >& /dev/null") != 0:
	restartAndSendEmailAlert(True, "smartproxy not running - restarted", "")
	sys.exit(1)

# check whether twistd is going bananas with memory consumption:
pid = commands.getoutput("cat /var/run/lounge/smartproxyd.pid")
memusage = int(commands.getoutput("grep VmRSS /proc/%s/status | awk '{ print $2 }'" % pid))
# if memusage is > 1 gig restart and include ops in the email alert:
if memusage > 1000000:
	restartAndSendEmailAlert(True, "smartproxy mem usage at %d kB - restarted" % memusage, "Memory usage is going bananas!!", True)
	sys.exit(1)


# get the smartproxy prefs:
prefs = Prefs('/etc/lounge/smartproxy.xml')

# call the admin script with the /check option (try 3 times before bailing):
# set the timeout to 60 secs:
socket.setdefaulttimeout(60)
for tries in range(3):
	try:
		file_response = urllib.urlopen("http://localhost:%s/ruok" % (prefs.get_pref('/http_port')))
		# if we got here, the previous call was successful, so exit:
		break
	except Exception, msg:
		if tries < 2:
			time.sleep(3) # sleep 3 seconds between retries
		else:
			# send email alert and restart if we can't connect to the admin script:
			restartAndSendEmailAlert(True, "smartproxy admin script not responding - restarted", "Python error: %s\n\nTraceback: %s" % (msg, traceback.format_exc()))
			sys.exit(1)

data = file_response.read()
if data != "imok":
	restartAndSendEmailAlert(False, "smartproxy - invalid response from admin script", "Python Error: %s\n\nTraceback: %s" % (msg, traceback.format_exc()))
	file_response.close()
	sys.exit(1)
