"""
Functions to help manage processes used in unittests.
This module registers a SIGCHLD handler the first time background() is called.
If something was registered on SIGCHLD already, it will call that handler 
after any process-specific callbacks have been executed.

* background(cmdline, exit_callback=None)
  Forks and execs the process specified by cmdline, returning the pid of the 
  child process.  If an exit_callback is specified, it will be called when the
  process exits.  See note below for arguments expected by exit_callback.

* stop_pids([pidlist]), sleep_between=0, clear_callbacks=True)
  SIGTERM then SIGKILL all the processes in the list -- wait sleep_between secs
  between sending all the SIGTERMs and sending the SIGKILLs.  clear_callbacks
  will prevent any callback functions registered from running.

* stop_pid(pid, sleep_between=0, clear_callbacks=True)
  convenience single-process version of stop_pids

* wait_for_connect(host, port, tries=50)
  Try to open up a socket to host:port -- if something is listening there,
  we'll take that to mean the process is up and running.  Will retry every
  tenth of a second, up to the retry limit.

* wait_for_process_exit(pid, tries=50):
  Wait until the entry for this process has been removed from the dict of 
  running processes, up until the maximum number of retries.

Exit Callback Functions
The callback specified in exit_callback should be something like:
	exit_callback(pid, exitcode, rusage)
where exitcode and rusage are the values returned by os.wait() -- check the
documentation there for details on how to work with those values.
"""

import atexit
import os
import signal
import socket
import time

from signal import signal, getsignal, SIGCHLD, SIGTERM, SIGKILL, SIG_DFL, SIG_IGN
from os import wait3, WNOHANG

keep = False
handler_init = False

exit_callbacks = {}
running_pids = {}

def cleanup_remaining_processes():
	for pid in running_pids:
		os.kill(pid, SIGTERM)
	for pid in running_pids:
		os.kill(pid, SIGKILL)

def master_sigchld_handler(pid, frame, previous_handler):
	global sigchld_handlers
	global waiting_for_exit

	while True:
		try:
			pid,exitcode,rusage  = wait3(WNOHANG)
		except:
			break

		if pid in running_pids:
			del running_pids[pid]

		if pid in exit_callbacks:
			f = exit_callbacks[pid]
			f(pid,exitcode,rusage)

		if previous_handler not in [SIG_DFL, SIG_IGN, None]:
			previous_handler(pid,frame)

def get_master_sigchld_handler(previous_handler):
	return lambda pid,frame:(master_sigchld_handler(pid, frame, previous_handler))

def init_sigchld_handler():
	global handler_init
	atexit.register(cleanup_remaining_processes)
	prev = getsignal(SIGCHLD)
	master_handler = get_master_sigchld_handler(prev)
	signal(SIGCHLD, master_handler)
	handler_init = True

def close_stdout_stderr():
	if keep:
		return
	# eat up stdout/stderr
	for fd in range(0, 1024):
		try:
			os.close(fd)
		except OSError: 
			pass
		
	os.open("/dev/null", os.O_RDWR)
	os.dup2(0, 1)
	os.dup2(0, 2)

def dont_keep_stdout_stderr():
	"""
	Subsequent background() calls will close stdout and stderr after forking.
	This is the default behavior of the module.
	"""
	global keep
	keep = False

def keep_stdout_stderr():
	"""
	Subsequent background() calls do NOT close stdout and stderr.
	"""
	global keep
	keep = True

def background(cmdline, exit_callback=None):
	"""
	Fork and execute a process specified by cmdline.  If successful, this 
	returns the pid of the newly spawned process.  If specified, bind a 
	callback to execute when the process exits.
	"""
	global handler_init
	global running_pids
	if not handler_init:
		init_sigchld_handler()
	pid = os.fork()
	if pid:
		if exit_callback:
			exit_callbacks[pid] = exit_callback
		running_pids[pid] = True
		return pid
	
	close_stdout_stderr()

	if type(cmdline)==type([]):
		args = cmdline
	else:
		args = cmdline.split()
	os.execv(args[0], args)
	# should never happen
	assert False, "exec failed"

def stop_pids(pidlist, sleep_between=0, clear_callbacks=True):
	"""
	Send SIGTERM to all the pids specified in pidlist.  Once all of the
	processes have been signaled, wait sleep_between seconds then send
	SIGKILL to all the processes.  
	"""
	for pid in pidlist:
		if clear_callbacks and pid in exit_callbacks:
			del exit_callbacks[pid]
		try:
			os.kill(pid, SIGTERM)
		except OSError:
			pass
	# wait before sending kill
	time.sleep(sleep_between)
	for pid in pidlist:
		try:
			os.kill(pid, SIGKILL)
		except OSError:
			pass

def stop_pid(pid, sleep_between=0, clear_callbacks=True):
	stop_pids([pid], sleep_between, clear_callbacks)

def wait_for_connect(host, port, tries=50):
	"""
	Try to open up a socket to host:port -- if something is listening there,
	we'll take that to mean the process is up and running.  Will retry every
	tenth of a second, up to the retry limit.
	"""
	while tries > 0:
		try:
			tries -= 1
			s = socket.socket()
			s.connect((host, port))
			return True
		except socket.error:
			time.sleep(0.1)
	return False

def wait_for_process_exit(pid, tries=50):
	"""
	Wait until the entry for this process has been removed from the dict of 
	running processes, up until the maximum number of retries.
	"""
	global running_pids
	cnt = 0
	while pid in running_pids:
		cnt += 1
		if cnt > tries:
			assert False, "Process never exited!"
		time.sleep(0.1)


if __name__ == "__main__":
	def orig_chld_handler(pid,f):
		print "original handler"
	def exit_handler(pid, exitcode, rusage):
		print "pid: %d exited" % pid

	signal(SIGCHLD, orig_chld_handler)
	background('ls', exit_handler)
	time.sleep(5)
# vi: noexpandtab ts=2 sts=2 sw=2
