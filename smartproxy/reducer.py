#Copyright 2009 Meebo, Inc.
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

import atexit
import copy
import cPickle
import lounge
import md5
import os
import PyICU
import random
import re
import sys
import time

import cjson

from zope.interface import implements

from twisted.python import log
from twisted.internet import defer
from twisted.internet import protocol, reactor, defer, process, task, threads
from twisted.internet.interfaces import IFinishableConsumer, IPushProducer, IPullProducer
from twisted.protocols import basic
from twisted.web import server, resource, client
from twisted.python.failure import DefaultException

import streaming

# see http://wiki.apache.org/couchdb/View_collation
json_type_order = {
	type(None): 0,
	type(False): 1,
	type(1): 2,
	type(1.1): 2,
	type("a"): 3,
	type(u"a"): 3,
	type([]): 4,
	type({}): 5 }

uca_collator = None
def uca_cmp(a, b):
	global uca_collator
	if uca_collator is None:
		uca_collator = PyICU.Collator.createInstance()
	return uca_collator.compare(a, b)

def json_list_cmp(a, b):
	minlen = min(len(a), len(b))
	for i in range(0, minlen):
		cval = json_cmp(a[i], b[i])
		if cval!=0:
			return cval

	# one is prefix of the other; shorter one comes first
	if minlen<len(b):
		return -1
	if minlen<len(a):
		return 1

	# they're equal
	return 0

def json_obj_cmp(a, b):
	# please don't use objects in your view keys. :(
	akeys = a.keys()
	bkeys = b.keys()
	akeys.sort(json_cmp)
	bkeys.sort(json_cmp)
	cval = json_list_cmp(akeys, bkeys)
	if cval != 0:
		return cval
	# same keys, compare values
	avals = [a[k] for k in akeys]
	bvals = [b[k] for k in bkeys]
	return json_list_cmp(avals, bvals)

def json_cmp(a, b):
	atypeidx = json_type_order[type(a)]
	btypeidx = json_type_order[type(b)]
	if atypeidx != btypeidx:
		return cmp(atypeidx, btypeidx)

	if isinstance(a, basestring):
		# use unicode collation algorithm
		return uca_cmp(a, b)

	if isinstance(a, list):
		return json_list_cmp(a, b)
	
	if isinstance(a, dict):
		return json_obj_cmp(a, b)

	return cmp(a,b)

def to_reducelist(stuff):
	return [row["value"] for row in stuff.get("rows",[])]

def split_by_key(rows):
	rv = []
	cur = []
	prev = None
	for row in rows:
		if row["key"]!=prev:
			if len(cur)>0:
				rv.append((prev, {"rows": cur}))
			cur = []
		cur.append(row)
		prev = row["key"]
	if len(cur)>0:
		rv.append((prev, {"rows": cur}))
	return rv

def dup_merge(rows1, rows2, compare=cmp):
	"""Merge the two results, preserving duplicates"""
	out = []
	i,j = 0,0
	while i<len(rows1) and j<len(rows2):
		if compare(rows1[i]["key"], rows2[j]["key"])<0:
			out.append(rows1[i])
			i += 1
		else:
			out.append(rows2[j])
			j += 1
	if i<len(rows1):
		out += rows1[i:]
	if j<len(rows2):
		out += rows2[j:]
	return out

def unique_merge(rows1, rows2, compare=cmp):
	"""Merge the results from r2 into r1, removing duplicates."""
	out = []
	i,j = 0,0
	while i<len(rows1) and j<len(rows2):
		if compare(rows1[i]["key"], rows2[j]["key"])<0:
			out.append(rows1[i])
		else:
			out.append(rows2[j])
		# advance both until we no longer match
		# TODO: manipulate total_rows when we find dups
		while i<len(rows1) and compare(rows1[i]["key"], out[-1]["key"])==0:
			i += 1
		while j<len(rows2) and compare(rows2[j]["key"], out[-1]["key"])==0:
			j += 1
	if i<len(rows1):
		out += rows1[i:]
	if j<len(rows2):
		out += rows2[j:]
	return out

def merge(r1, r2, compare=json_cmp, unique=False, descending=False):
	"""Merge the results from r2 into r1."""
	rows1 = r1["rows"]
	rows2 = r2["rows"]
	merge_fn = unique and unique_merge or dup_merge
	if descending:
		real_compare = compare
		compare = lambda x, y: real_compare(y, x)
	r1["rows"] = merge_fn(rows1, rows2, compare)
	if "total_rows" in r2:
		if not ("total_rows" in r1):
			r1["total_rows"] = 0
		r1["total_rows"] += r2["total_rows"]
	if "offset" in r2:
		if not ("offset" in r1):
			r1["offset"] = 0
		r1["offset"] = r2["offset"]
	return r1

class ReduceQueue:
	def __init__(self, prefs):
		self.queue = []
		self.pool = []
		self.started = False
		self.pool_size = prefs.get_pref("/reduce_pool_size")
		self.process = prefs.get_pref("/couchjs_command").split()
	
	def start_reducers(self):
		# we can't do this until after the reactor starts.
		for i in range(self.pool_size):
			rPP = ReducerProcessProtocol()
			# TODO figure how to put this in the constructor
			rPP.reduce_queue = self
			reactor.spawnProcess(rPP, self.process[0], self.process)
		self.started = True

	def enqueue(self, keys, lines, cb):
		# Accept some data for the reducer pool.
		if not self.started:
			self.start_reducers()
		self.queue.append((keys, lines, cb))
		self.next()
	
	def return_to_pool(self, reducer):
		# A reducer calls this when it's finished.
		self.pool.append(reducer)
		self.next()
	
	def next(self):
		# if we have something in the queue, and an available reducer, take care of it
		if len(self.queue)>0 and len(self.pool)>0:
			keys, lines, cb = self.queue.pop(0)
			reducer = self.pool.pop(0)

			def reduce_finished(response):
				self.return_to_pool(reducer)
				log.debug("ReduceQueue: success, queue size %d, pool size %d" % (len(self.queue), len(self.pool)))
				cb(response)

			def reduce_failed(*args, **kwargs):
				self.return_to_pool(reducer)
				log.debug("ReduceQueue: failure, queue size %d, pool size %d" % (len(self.queue), len(self.pool)))

			reducer.feed(keys, lines, reduce_finished, reduce_failed)
		else:
			log.debug("ReduceQueue: success, queue size %d, pool size %d" % (len(self.queue), len(self.pool)))

class ReducerProcessProtocol(protocol.ProcessProtocol):
	def feed(self, keys, lines, fn, err_fn):
		self._deferred = defer.Deferred()
		self._deferred.addCallback(fn)
		self._deferred.addErrback(err_fn)

		self.keys = keys
		self.lines = lines
		self.response = ""

		if len(self.lines)==0:
			log.debug("nothing to reduce")
			self._deferred.callback( ([], "") )
			return

		for line in self.lines:
			log.debug("Sending line to reducer %s" % line)
			self.transport.writeToChild(0, line + "\r\n")
		log.debug("done sending data")

	def connectionMade(self):
		# tell the reduce queue we are ready for action
		self.reduce_queue.return_to_pool(self)

	def childDataReceived(self, childFD, response):
		log.debug("Received data from reducer %s" % response)
		if childFD == 1:
			self.response += response
			# should get one line back for each line we sent (plus one for trailing newline)
			response_lines = len(self.response.split("\n"))
			if response_lines>len(self.lines):
				self._deferred.callback( (self.keys, self.response) )

class Reducer:
	def __init__(self, reduce_func, num_entries, args, deferred, reduce_queue):
		self.reduce_func = reduce_func
		self.num_entries_remaining = num_entries
		self.queue = []
		self.reduce_deferred = deferred
		self.reduces_out = 0
		self.count = None
		self.skip = 0
		if 'limit' in args:
			self.count = int(args['limit'][0])
		elif 'count' in args:
			self.count = int(args['count'][0])
		if 'skip' in args:
			self.skip = int(args['skip'][0])
		self.reduce_queue = reduce_queue
		self.coderecvd = None
		self.headersrecvd = {}

		self.descending = 'true' in args.get('descending', ['false'])
		self.etags = {}

	def process_map(self, data, shard=None, headers={}, code=None):
		if code is not None:
			self.coderecvd = code
		if shard is not None:
			etag = [vs[-1] for (k,vs) in headers.iteritems() if k.lower() == 'etag']
			etag = etag and etag[0].strip('"') or ''
			self.etags[shard] = etag
			log.debug("process_map: etag for shard %s is %s" % (shard, etag))
		self.headersrecvd.update(headers)

		#TODO: check to make sure this doesn't go less than 0
		self.num_entries_remaining = self.num_entries_remaining - 1
		try:
			results = cjson.decode(data)
		except:
			log.err('Could not json decode: %s' % data)
			results = {'total_rows': 0, 'offset': 0, 'rows': []}
		#result => {'rows' : [ {key: key1, value:value1}, {key:key2, value:value2}]}
		self.queue_data(results)

	def process_reduce(self, args):
		self.reduces_out -= 1
		keys, data = args
		entries = data.split("\n")
		log.debug("in process reduce: %s %s" % (keys, entries))
		results = [cjson.decode(entry) for entry in entries if len(entry) > 0]
		#keys = [key1, key2]
		#results = [ [success_for_key1, [value_from_fn1, value_from_fn2]], [success_for_key2, [value_from_fn1, value_from_fn2]]]
		r = []
		for k, v in zip(keys, [val[0] for s,val in results]):
			r.append( dict(key=k, value=v) )
		self.queue_data(dict(rows=r))

	def queue_data(self, data):
		self.queue.append(data)
		self.__reduce()
	
	def _do_reduce(self, a, b):
		"""Actually combine two documents into one.

		Override this to get different reduce behaviour.
		"""
		inp = merge(a, b, descending=self.descending) #merge two sorted lists together

		if self.reduce_func:
			args = [ (key, ["rereduce", [self.reduce_func], to_reducelist(chunk)]) for key,chunk in split_by_key(inp["rows"])]
			lines = [cjson.encode(chunk) for key, chunk in args]
			keys = [key for key,chunk in args]
			#TODO: maybe this could be lines,keys = zip(*(key, cjson.encode(chunk) for key, chunk in args))
			self.reduces_out += 1
			self.reduce_queue.enqueue(keys, lines, self.process_reduce)
		else:
			# no reduce function; just merge
			self.queue_data(inp)

	def __reduce(self):
		"""Pull stuff off the queue."""
		#only need to reduce if there is more than one item in the queue
		if len(self.queue) == 1:
			#if we've received all the results from all the shards
			#and the queue only has 1 element in it, then we're done reducing
			if self.num_entries_remaining == 0 and self.reduces_out == 0:
				# if this was a count query, slice stuff off
				if self.count is not None:
					log.debug("count: %d, skip: %d, results: %d" % (self.count, self.skip, len(self.queue[0]['rows'])))
					self.queue[0]['rows'] = self.queue[0]['rows'][self.skip:self.skip+self.count]
				elif self.skip > 0:
					self.queue[0]['rows'] = self.queue[0]['rows'][self.skip:]
				body = cjson.encode(self.queue[0])
				# filter headers that should not be reverse proxied
				strip_headers = ['content-length', 'etag']
				headers = dict([(k,v) for k,v in self.headersrecvd.iteritems() if k.lower() not in strip_headers])
				
					# calculate a deterministic etag
				nodes = self.etags.keys()
				nodes.sort() # sum in deterministic order
				md5etag = md5.md5()
				for node in nodes:
					md5etag.update(self.etags[node])
				if len(nodes) > 0:
					headers['etag'] = ['"%s"' % md5etag.hexdigest()]
				log.debug("Reducer: response headers = %s" % str(headers))
				self.reduce_deferred.callback((self.coderecvd, headers, body))
			return

		a,b = self.queue[:2]
		self.queue = self.queue[2:]
		# hand the work off to _do_reduce, which we can override
		self._do_reduce(a,b)

	def get_deferred(self):
		return self.reduce_deferred

class AllDocsReducer(Reducer):
	def _do_reduce(self, a, b):
		# merge and unique.  no reduce
		self.queue_data(merge(a, b, unique=True, descending=self.descending))

class BulkDocsReducer(Reducer):
	def _do_reduce(self, a, b):
		"""Mash two arrays together"""
		self.queue_data(a + b)

class ChangesReducer(Reducer):
	def __init__(self, seq, deferred):
		self._sequence = seq
		self._results = []
		self._lastseq = [0 for x in seq]
		self._response_headers = None
		Reducer.__init__(self, None, len(self._sequence), {}, deferred, None)

	def process_map(self, data, shard, headers):
		#TODO: check to make sure this doesn't go less than 0
		log.msg("Got headers: %s" % str(headers))
		if not self._response_headers:
			self._response_headers = copy.copy(headers)
		self.num_entries_remaining -= 1
		mo = re.search(r'(\d+)$', shard)
		shard_idx = int(mo.group(1))
		results = cjson.decode(data)
		self.queue_data(shard_idx, results)
	
	def queue_data(self, shard_idx, a):
		"""Mash _changes sequences together."""
		if "last_seq" in a:
			self._lastseq[shard_idx] = a["last_seq"]
		for change in a["results"]:
			self._sequence[shard_idx] = change["seq"]
			change["seq"] = cjson.encode(self._sequence)
			self._results.append(change)

		if self.num_entries_remaining == 0:
			self.reduce_deferred.callback((200, self._response_headers, cjson.encode({"results": self._results, "last_seq": cjson.encode(self._lastseq)})))

# vi: noexpandtab ts=2 sts=2 sw=2
