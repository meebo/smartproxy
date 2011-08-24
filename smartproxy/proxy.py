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
import cjson
import cPickle
import copy
import itertools
import lounge
import os
import random
import re
import sys
import time
import urllib
import zlib

from zope.interface import implements

from twisted.python import log
from twisted.internet import defer
from twisted.internet import protocol, reactor, defer, process, task, threads
from twisted.internet.interfaces import IConsumer
from twisted.protocols import basic
from twisted.web import server, resource, client, error, http, http_headers
from twisted.python.failure import DefaultException

from fetcher import HttpFetcher, MapResultFetcher, DbFetcher, DbGetter, ViewFetcher, AllDbFetcher, ProxyFetcher, ChangesFetcher, UuidFetcher, getPageWithHeaders, getPageFromAny, getPageFromAll

from reducer import ReduceQueue, ReducerProcessProtocol, Reducer, AllDocsReducer, ChangesReducer, BulkDocsReducer

import changes
import streaming
import reducer

from lrucache import LRUCache

def lounge_hash(x):
	# encode the doc_id as utf8 since this is how dumbproxy gets it
	# urls with unicode characters turn into %-escaped unicode pairs
	
	# the & 0xffffffff guarantees we get the same result regardless
	# of the sign since this behavior changed between python versions
	crc = zlib.crc32(x.encode('utf8'), 0) & 0xffffffff
	return crc

def which_shard(x, n_shards):
	return min(n_shards - 1, x/(0x100000000 / n_shards))

def normalize_header(h):
	return '-'.join([word.capitalize() for word in h.split('-')])

qsre = re.compile('([^&])+=([^&]*)(?=&|$)')
def qsparse(qs):
	return dict((map(urllib.unquote, pair.groups()) for pair in qsre.finditer(qs)))

def should_regenerate(now, cached_at, cachetime):
	age = now - cached_at
	time_left = cachetime - age
	# probably is 0.0 at 5 minutes before expire, 1.0 at expire time, linear
	# in between
	if time_left > 5*60:
		return False
	if time_left <= 0:
		return True
	p = 1 - (time_left/float(5*60))
	return random.random() < p

def get_body(request, default=None):
	if hasattr(request.content, 'getvalue'):
		# request is a StringIO. twistd probably already parsed it based on content-type
		body = request.content.getvalue()
	else:
		if hasattr(request.content, 'seek'):
			request.content.seek(0)
		body = request.content.read()
	if body:
		body = cjson.decode(body)
	else:
		body = default
	return body

class ClientQueue:
	def __init__(self, prefs):
		self.queue = []
		self.pool_size = prefs.get_pref("/client_pool_size")
		self.count = 0
	
	def enqueue(self, url, good_cb, bad_cb, headers=None):
		self.queue.append((url, good_cb, bad_cb, headers))
		self.next()
	
	def next(self):
		# if we have something in the queue, and an available reducer, take care of it
		if len(self.queue)>0 and self.count < self.pool_size:
			url, success, err, headers = self.queue.pop(0)

			def succeed(*args, **kwargs):
				log.debug("ClientQueue: success, queue size %d, reqs out %d" % (len(self.queue), self.count))
				self.count -= 1
				try:
					success(*args, **kwargs)
				except Exception, e:
					log.err("Exception '%s' in ClientQueue callback; moving on" % str(e))
				self.next()

			def fail(*args, **kwargs):
				log.debug("ClientQueue: failure, queue size %d, reqs out %d" % (len(self.queue), self.count))
				self.count -= 1
				try:
					err(*args, **kwargs)
				except:
					log.err("Exception in ClientQueue errback; moving on")
				self.next()

			self.count += 1
			defer = getPageWithHeaders(url, headers=headers).deferred
			defer.addCallback(succeed)
			defer.addErrback(fail)
		else:
			log.debug("ClientQueue: queue size %d, reqs out %d" % (len(self.queue), self.count))

def make_success_callback(request):
	def send_output(params):
		code, headers, doc = params
		for k in headers:
			if len(headers[k])>0:
				request.setHeader(normalize_header(k), headers[k][0])
		request.setHeader('Content-Length', str(len(doc)))
		request.setResponseCode(code)
		request.write(doc)
		request.finish()
	return send_output

def make_errback(request):
	def handle_error(reason):
		# First unpack any nested Failures from DeferredList
		try:
			while True:
				reason.trap(defer.FirstError)
				reason = reason.value.subFailure
		except:
			pass

		# Then check the reason and return a response
		if reason.check(error.Error):
			if hasattr(reason.value, 'message'):
				request.setResponseCode(int(reason.value.status),
										reason.value.message)
			else:
				request.setResponseCode(int(reason.value.status))
			if hasattr(reason.value, 'response') and reason.value.response:
				request.write(reason.value.response)
			request.finish()
		else:
			request.processingFailed(reason)
	return handle_error		

class SmartproxyResource(resource.Resource):
	isLeaf = True

	def __init__(self, prefs):
		"""
		prefs is a lounge.prefs.Prefs instance
		persistCache is a boolean -- True means an atexit handler will be
		  registered to persist the cache to disk when the process 
			finishes.
		"""
		self.prefs = prefs

		self.reduce_queue = ReduceQueue(self.prefs)
		self.client_queue = ClientQueue(self.prefs)

		self.__last_load_time = 0
		self.__loadConfig()
		self.__loadConfigCallback = task.LoopingCall(self.__loadConfig)
		self.__loadConfigCallback.start(300)

		self.in_progress = {}

	def __loadConfig(self):
		conf_file = self.prefs.get_pref("/proxy_conf")
		mtime = os.stat(conf_file)[8]
		if mtime <= self.__last_load_time:
			return
		self.__last_load_time = mtime
		self.conf_data = lounge.ShardMap(conf_file)

		try:
			cache_conf = self.prefs.get_pref('/cacheable_file_path')
			cacheables = cjson.decode(file(cache_conf).read())
			self.cacheable = [(re.compile(pat), t) for pat, t in cacheables]
		except KeyError:
			self.cacheable = []

		try:
			cache_size = int(self.prefs.get_pref('/cache_entries'))
		except KeyError:
			cache_size = 16

		if hasattr(self, 'cache'):
			self.cache.size = cache_size
		else:
			self.cache = LRUCache(cache_size)

	def render_temp_view(self, request):
		"""Farm out a view query to all nodes and combine the results."""
		deferred = defer.Deferred()
		deferred.addCallback(make_success_callback(request))
		deferred.addErrback(make_errback(request))

		body = get_body(request)
		reduce_fn = body.get("reduce", None)
		if reduce_fn is not None:
			reduce_fn = reduce_fn.replace("\n", " ") # TODO do we need this?

		uri = request.uri[1:]
		db, req = uri.split('/', 1)
		shards = self.conf_data.shards(db)
		reducer = Reducer(reduce_fn, len(shards), {}, deferred, self.reduce_queue)

		failed = False
		for shard in shards:
			def succeed(data):
				log.err("This should not get called?")
				pass

			def fail(data):
				if not failed:
					failed = True
					deferred.errback(data)

			shard_deferred = defer.Deferred()
			shard_deferred.addCallback(succeed)
			shard_deferred.addErrback(fail)

			nodes = self.conf_data.nodes(shard)
			urls = ['/'.join([node, req]) for node in nodes]
			fetcher = MapResultFetcher(shard, urls, reducer, deferred, self.client_queue)
			fetcher.fetch(request)

		return server.NOT_DONE_YET

	def render_view(self, request):
		"""Farm out a view query to all nodes and combine the results."""
		path = request.path[1:]
		parts = path.split('/')
		database = parts[0]
		if parts[1] == '_design':
			view = parts[4]
			design_doc = '/'.join([parts[1], parts[2]])
		else:
			view = parts[3]
			design_doc = "_design%2F" + parts[2]

		path_no_db = path[ path.find('/')+1:]
			
		#  old:  http://host:5984/db/_view/designname/viewname
		#  new:  http://host:5984/db/_design/designname/_view/viewname

		# build the query string to send to shard requests
		strip_params = ['skip'] # handled by smartproxy, do not pass to upstream nodes
		if 'skip' in request.args:
			strip_params.append('limit') # have to handle limit in smartproxy -- SLOW!!!
		args = dict([(k,v) for k,v in request.args.iteritems() if k.lower() not in strip_params])
		qs = urllib.urlencode([(k,v) for k in args for v in args[k]] or '')
		if qs: qs = '?' + qs

		# get the urls for the shard primary replicas
		primary_urls = [self._rewrite_url("/".join([host, design_doc])) for host in self.conf_data.primary_shards(database)]
		view_uri = request.path.split("/",2)[2] + qs

		#if we're already processing a request for this uri, just append this
		#request to the list -- it will be handled with the results from 
		#the previous request.
		if request.uri in self.in_progress:
			self.in_progress[request.uri].append(request)
			log.debug("Attaching request for %s to an earlier request for that uri" % request.uri)
		else:
			self.in_progress[request.uri] = [request]

		# define the callback functions
		def send_output(s):
			code, headers, response = s

			# write the response to all requests
			clients = self.in_progress.pop(request.uri, [])

			for c in clients:
				for k in headers:
					c.setHeader(k, headers[k][-1])
				c.write(response+"\n")
				c.finish()
			return s

		def handle_error(s):
			# send the error to all requests
			clients = self.in_progress.pop(request.uri, [])

			# if we get back some non-http response type error, we should
			# return 500
			if hasattr(s.value, 'status'):
				status = int(s.value.status)
			else:
				status = 500
			if hasattr(s.value, 'response'):
				response = s.value.response
			else:
				response = cjson.encode(dict(error=str(s)))

			for c in clients:
				c.setResponseCode(status)
				c.write(response+"\n") 
				c.finish()
			
			# make sure to return the failure to skip any callbacks
			return s

		# callback to insert on the chain if the response should be cached
		def cache_output(s):
			code, headers, response = s
			if code == 200:
				self.cache[request.uri] = (time.time(), s)
			return s

		# predicate check for whether path matches a cache configuration pattern
		def cache_pred(x):
			pattern, cachetime = x
			return pattern.match(request.path)

		# create the deferred object to hold the callbacks
		deferred = defer.Deferred()

		try:
			# don't look for cache pattern matches if stale data is not ok
			if 'ok' not in request.args.get('stale', []):
				raise StopIteration

			# otherwise check the cache predicate against known cacheable patterns
			match = itertools.dropwhile(lambda x: not cache_pred(x), self.cacheable)
			pattern, cachetime = match.next() # raises StopIteration

			# if there is a cached response, check if it should be regenerated
			# and send the cached response no matter what
			if request.uri in self.cache:
				cached_at, response = self.cache[request.uri]
				# see if it is yet to expire
				now = time.time()
				cached_response = response

				# send the cached copy back to the client synchronously
				log.debug("Using cached copy of " + request.uri)
				send_output(response)

				# if the cache is stale and there are no other requests for this uri,
				# set a callback to cache the response when we fetch it
				if should_regenerate(now, cached_at, cachetime):
					log.debug("Cache of " + request.uri + " is stale. Regenerating.")
					deferred.addCallback(cache_output)
				else:
					return server.NOT_DONE_YET
			else:
				deferred.addCallback(cache_output)
				deferred.addCallbacks(send_output, handle_error)
		except StopIteration:
			# not cacheable, so set up our callback and make the request
			deferred.addCallbacks(send_output, handle_error)
			
		r = ViewFetcher(self.conf_data, primary_urls, database, view_uri, view, deferred, self.client_queue, self.reduce_queue)
		try:
			r.fetch(request)
		except:
			# make sure we clear this out if an exception is thrown
			self.in_progress.pop(request.uri, None)
		
		return server.NOT_DONE_YET

	def render_changes(self, request):
		database = request.path[1:].split('/', 1)[0]
		shards = dict(map(lambda s:
						  (self.conf_data.get_index_from_shard(s), s),
						  self.conf_data.shards(database)))

		feed = request.args.get('feed',['nofeed'])[-1]
		continuous = (feed == 'continuous')

		since = None
		if 'since' in request.args and request.args['since'][-1] != '0':
			since = changes.decode_seq(request.args['since'][-1])
			if True in itertools.imap(
				lambda s: str(s) not in since # missing shard
				or True not in itertools.imap(lambda r:
					str(r) in since[str(s)],
					self.conf_data.shardmap[s]), # no recognized replicas
				shards.iterkeys()):
				request.setResponseCode(http.BAD_REQUEST)
				return ('{"error":"bad request",'
					    '"reason":'
					    '"missing shard or bad replicas in since"}')
			del request.args['since']
		else:
			since = dict(
				map(lambda n: (str(n),
							   {str(self.conf_data.shardmap[n][0]): 0}),
					shards))

		heartbeat = None
		if 'heartbeat' in request.args and continuous:
			heartbeat = task.LoopingCall(lambda: request.write('\n'))
			if request.args['heartbeat'][-1] == 'true':
				heartbeat.start(60) # default 1 bpm
			else:
				heartbeat.start(int(request.args['heartbeat'][-1]) / 1000)
			request.args['heartbeat'] = ['true'] # uses httpd:changes_timeout config in couch
			
		kwargs = {'headers': request.getAllHeaders()}
		if(request.getHeader('accept') == 'application/json'):
			request.setHeader('content-type', 'application/json')
		else:
			request.setHeader('content-type', 'text/plain;charset=utf8')

		input_xform, output_xform = changes.transformations(continuous)
		json_output = streaming.LinePCP(request, xform = output_xform)
		shard_proxy = changes.ChangesProxy(json_output, since)

		deferred_shards = []

		def cleanup_changes(reason):
			try:
				reason.trap(error.Error) # trap http error
				request.setResponseCode(int(reason.value.status))
				if reason.value.status == '404':
					request.write('{"error": "not_found", "reason": "no_db_file"}')
					request.write('\n')
				else:
					request.write('{"error": "unknown"}') # good enough
					request.write('\n')
					reason.raiseException()
			except:
				# otherwise clean closure
				shard_proxy.finish() # writes last_seq


		def finish_changes(results):
			# wrap try/finally for python 2.4 compatibility
			try:
				if continuous:
					reason, shard_id = results # packed by deferred list
					reason, ch_idx = reason # packed by deferred list
					reason, rep_id, factory = reason # packed by getPageFromAll
					if heartbeat:
						heartbeat.stop()
					# stop the remaining channels
					shard_proxy.stopProducing()
					cleanup_changes(reason)
				else:
					shard_proxy.finish()
			finally:
				json_output.finish()
				request.unregisterProducer()
				request.finish()

		for shard_id, rep_since in since.iteritems():
			qs = urllib.urlencode([(k,v) for k in request.args for v in request.args[k]])

			def urlGen():
				for node, seq in rep_since.iteritems():
					host, port = self.conf_data.nodelist[int(node)]
					yield ("http://%s:%d/%s/_changes?since=%s&%s" %
					       (host, port, shards[int(shard_id)], seq, qs)).strip('&')
			shard_channel = shard_proxy.createChannel(shard_id)
			rep_proxy = changes.ChangesProxy(shard_channel,
							 since[shard_id])

			channels = (streaming.LinePCP(rep_proxy.createChannel(rep),
						      xform = input_xform)
				    for rep in rep_since.iterkeys())

			deferred_reps = getPageFromAll(
				itertools.izip(
					rep_since.iterkeys(),
					urlGen(),
					([c] for c in channels),
					itertools.repeat(kwargs)),
				factory=streaming.HTTPLineProducer)

			deferred_reps_list = defer.DeferredList(
				deferred_reps,
				fireOnOneErrback=1,
				fireOnOneCallback=1,
				consumeErrors=1)
			deferred_shards.append(deferred_reps_list)

		failfast = continuous and 1 or 0
		deferred = defer.DeferredList(deferred_shards,
					      fireOnOneErrback=failfast,
					      fireOnOneCallback=failfast,
					      consumeErrors=1)
		deferred.addBoth(finish_changes)
		return server.NOT_DONE_YET
	
	def render_all_docs(self, request):
		# /database/_all_docs?stuff => database, _all_docs?stuff
		database, rest = request.path[1:].split('/', 1)
		deferred = defer.Deferred()
		deferred.addCallback(make_success_callback(request))
		deferred.addErrback(make_errback(request))

		# build the query string to send to shard requests
		strip_params = ['skip'] # handled by smartproxy, do not pass
		args = dict([(k,v) for k,v in request.args.iteritems() if k.lower() not in strip_params])
		qs = urllib.urlencode([(k,v) for k in args for v in args[k]] or '')
		if qs: qs = '?' + qs

		# this is exactly like a view with no reduce
		shards = self.conf_data.shards(database)
		reducer = AllDocsReducer(None, len(shards), request.args, deferred, self.reduce_queue)

		#hash keys
		numShards = len(shards)
		shardContent = [[] for x in shards]
		body = get_body(request, {})
		if 'keys' in body:
			for key in body['keys']:
				where = which_shard(lounge_hash(key), numShards)
				shardContent[where].append(key)

		for i,shard in enumerate(shards):
			nodes = self.conf_data.nodes(shard)
			urls = [self._rewrite_url("/".join([node, rest])) + qs for node in nodes]
			shardBody =cjson.encode(dict(keys=shardContent[i]))
			fetcher = MapResultFetcher(shard, urls, reducer, deferred, self.client_queue, body=shardBody)

			fetcher.fetch(request)

		return server.NOT_DONE_YET
	
	def proxy_special(self, request):
		"""Proxy a special document to a single shard (any shard will do, but start with the first)"""
		deferred = defer.Deferred()

		def handle_success(params):
			code, headers, doc = params
			for k in headers:
				if len(headers[k])>0:
					request.setHeader(k, headers[k][0])
			request.setResponseCode(code)
			request.write(doc)
			request.finish()
		deferred.addCallback(handle_success)

		def handle_error(s):
			# if we get back some non-http response type error, we should
			# return 500
			if hasattr(s.value, 'status'):
				status = int(s.value.status)
			else:
				status = 500
			if hasattr(s.value, 'response'):
				response = s.value.response
			else:
				response = '{}'
			request.setResponseCode(status)
			request.write(response+"\n") 
			request.finish()
		deferred.addErrback(handle_error)

		database, rest = request.path[1:].split('/',1)
		_primary_urls = ['/'.join([host, rest]) for host in self.conf_data.primary_shards(database)]
		primary_urls = [self._rewrite_url(url) for url in _primary_urls]
		fetcher = ProxyFetcher("proxy", primary_urls, deferred, self.client_queue)
		fetcher.fetch(request)
		return server.NOT_DONE_YET

	def render_GET(self, request):
		if request.uri == "/ruok":
			request.setHeader('Content-Type', 'text/html')
			return "imok"

		if request.uri == "/_smartproxy_stats":
			request.setHeader('Content-Type', 'text/plain')
			status = """ClientQueue size: %d\nClient reqs out: %d\nReduceQueue size: %d\nReduce pool size: %d\n\nGETs in progress:\n""" % (len(self.client_queue.queue), self.client_queue.count, len(self.reduce_queue.queue), len(self.reduce_queue.pool))
			for uri in self.in_progress:
				status += "  * %s (%d)\n" % (uri, len(self.in_progress[uri]))
			return status + "\n"

		if request.uri == "/_all_dbs":
			db_urls = [self._rewrite_url(host + "_all_dbs") for host in self.conf_data.nodes()]
			deferred = defer.Deferred()
			deferred.addCallback(lambda s:
				(request.write(cjson.encode(s)+"\n"), request.finish()))
			all = AllDbFetcher(self.conf_data, db_urls, deferred, self.client_queue)
			all.fetch(request)
			return server.NOT_DONE_YET

		# GET /db
		if '/' not in request.uri.strip('/'):
			return self.get_db(request)

		# GET /db/_all_docs .. or ..
		# GET /db/_all_docs?options
		if request.path.endswith("/_all_docs"):
			return self.render_all_docs(request)

		# GET /db/_changes .. or ..
		# GET /db/_changes?options
		if request.path.endswith("/_changes"):
			return self.render_changes(request)

		if '/_view/' in request.uri:
			return self.render_view(request)

		# GET /db/_somethingspecial
		if re.match(r'/[^/]+/_.*', request.uri):
			return self.proxy_special(request)

		return cjson.encode({"error": "smartproxy is not smart enough for that request"})+"\n"
	
	def render_PUT(self, request):
		"""Handle a special PUT (design doc or database)"""
		# PUT /db/_somethingspecial
		if re.match(r'/[^/]+/_.*', request.uri):
			return self.proxy_special(request)
			#return cjson.encode({"stuff":"did not happen"})+"\n"

		# create all shards for a database
		if '/' not in request.uri.strip('/'):
			return self.do_db_op(request)

		return cjson.encode({"error": "smartproxy is not smart enough for that request"})+"\n"
	
	def do_bulk_docs(self, request):
		"""Split and farm out bulk docs"""
		# /database/_all_docs?stuff => database, _all_docs?stuff
		database, rest = request.path[1:].split('/', 1)
		deferred = defer.Deferred()
		deferred.addCallback(make_success_callback(request))
		deferred.addErrback(make_errback(request))

		# build the query string to send to shard requests
		args = dict([(k,v) for k,v in request.args.iteritems()])
		qs = urllib.urlencode([(k,v) for k in args for v in args[k]] or '')
		if qs: qs = '?' + qs

		# this is exactly like a view with no reduce
		shards = self.conf_data.shards(database)
		reducer = BulkDocsReducer(None, len(shards), request.args, deferred, self.reduce_queue)

		#hash keys
		numShards = len(shards)
		shardContent = [[] for x in shards]
		body = get_body(request, {})
		if 'docs' in body:
			for doc in body['docs']:
				doc_id = doc['_id']
				where = which_shard(lounge_hash(doc_id), numShards)
				shardContent[where].append(doc)

		for i,shard in enumerate(shards):
			nodes = self.conf_data.nodes(shard)
			urls = [self._rewrite_url("/".join([node, rest])) + qs for node in nodes]
			shardBody = cjson.encode(dict(body.items(), docs=shardContent[i]))

			fetcher = MapResultFetcher(shard, urls, reducer, deferred, self.client_queue, body=shardBody)
			fetcher.fetch(request)

		return server.NOT_DONE_YET

	def do_missing_revs(self, request):
		def finish_missing_revs(results):
			def combine_results(acc, shard_result):
				# DeferredList packs success as (True, result)
				shard_result = shard_result[1]
				# getPageFromAny packs result as (result, identifier, factory)
				shard_result, shard_idx, factory = shard_result
				acc.update(cjson.decode(shard_result)['missing_revs'])
				return acc
			all_results = reduce(combine_results, results, dict())
			output = {'missing_revs': all_results}
			log.msg(cjson.encode(output) + '\n')
			request.write(cjson.encode(output) + '\n')
			request.finish()

		database, rest = request.path[1:].split('/', 1)
		shards = self.conf_data.shards(database)

		#sort the docs into shard buckets by hashing the keys
		numShards = len(shards)
		shardContent = [{} for x in shards]
		body = get_body(request, {})
		for doc_id in body:
			where = which_shard(lounge_hash(doc_id), numShards)
			shardContent[where][doc_id] = body[doc_id]

		deferred = defer.DeferredList(
			[getPageFromAny([
				(shard_idx,
				 "/".join([shard_uri, rest]),
				 [],
				 { 'method': 'POST',
				   'postdata': cjson.encode(shardContent[shard_idx])})
				for shard_uri in self.conf_data.nodes(shard)])
			 for shard_idx, shard in enumerate(shards)],
			fireOnOneErrback=1,
			fireOnOneCallback=0,
			consumeErrors=1)
		deferred.addCallback(finish_missing_revs)
		deferred.addErrback(make_errback(request))
		
		return server.NOT_DONE_YET

	def do_ensure_full_commit(self, request):
		"""
		TODO: We should pass the instance_start_time back
		CouchDB uses this to ensure that the target database
		didn't crash between checkpoints during replication.

		In our case, we strip it from here and the /db info
		response. This causes replication to see undefined
		in both cases, which always matches.

		This is safe so long as the shard configuration does
		not change and we use delayed_commits=false or a
		battery-backed cache on every node.

		The more general solution is to request all available
		information by contacting all replicas of all shards
		and forcing replication to retry from the last checkpoint
		any time the instance_start_time vector does not match.

		However, we don't want to do this until we feel confident
		that we can store checkpoints in a redundant way. This means
		opening up the read repain can of worms or making smartproxy
		a real replication middle-man with its own database for
		storing and replicating cluster-wide checkpoint logs.
		"""
		def finish_request(results):
			request.setResponseCode(201)
			request.setHeader('Content-Length', 12)
			request.write('{"ok":true}\n')
			request.finish()

		database, rest = request.path[1:].split('/', 1)
		shards = self.conf_data.shards(database)

		deferred = defer.DeferredList(
			[getPageFromAny([
				(shard_idx,
				 '/'.join([shard_uri, rest]),
				 [],
				 { 'method': 'POST',
				   'headers': request.getAllHeaders()
				   })
				for shard_uri in self.conf_data.nodes(shard)])
			 for shard_idx, shard in enumerate(shards)],
			fireOnOneErrback=1,
			fireOnOneCallback=0,
			consumeErrors=1)
		deferred.addCallback(finish_request)
		deferred.addErrback(make_errback(request))

		return server.NOT_DONE_YET

	def render_POST(self, request):
		"""Create all the shards for a database."""
		db, rest = request.uri[1:], None
		if '/' in db:
			db, rest = request.uri[1:].split('/',1)
			if rest=='':
				rest = None

		# POST /db/_temp_view .. or ..
		# POST /db/_temp_view?options
		if request.uri.endswith("/_temp_view") or ("/_temp_view?" in request.uri):
			return self.render_temp_view(request)

		if request.path.endswith("/_all_docs"):
			return self.render_all_docs(request)

		# POST /db/_ensure_full_commit
		if request.uri.endswith("/_ensure_full_commit"):
			return self.do_ensure_full_commit(request)

		if request.uri.endswith("/_bulk_docs") or ("/_bulk_docs?" in request.uri):
			return self.do_bulk_docs(request)

		if request.uri.endswith("/_missing_revs"):
			return self.do_missing_revs(request)

		# PUT /db/_somethingspecial
		if re.match(r'/[^/]+/_.*', request.uri):
			return self.proxy_special(request)

		if rest is None:
			return self.create_doc(request)

		return cjson.encode({"error": "smartproxy is not smart enough for that request"})+"\n"
	
	def render_DELETE(self, request):
		"""Delete all the shards for a database."""
		return self.do_db_op(request)
	
	def create_doc(self, request):
		"""Create a document via POST.

		1. Ask any node for a UUID
		2. Hash the UUID to find a shard
		3. PUT the document to that shard
		"""
		db_name = request.uri[1:]
		nodes = self.conf_data.nodes()
		urls = [node + '_uuids' for node in nodes]

		body = request.content.read()
		deferred = defer.Deferred()
		deferred.addCallback(make_success_callback(request))
		deferred.addErrback(make_errback(request))
		fetcher = UuidFetcher(db_name, urls, deferred, body, self.conf_data)

		doc = cjson.decode(body)
		if '_id' in doc:
			fetcher.put_doc(doc['_id'])
		else:
			log.msg("create_doc " + request.uri + " fetching")
			fetcher.fetch(request)

		return server.NOT_DONE_YET
	
	def do_db_op(self, request):
		"""Do an operation on each shard in a database."""
		# chop off the leading /
		uri = request.path[1:]
		if '/' in uri:
			db_name, rest = uri.split('/', 1)
		else:
			db_name, rest = uri, None
		if rest=='':
			rest = None

		# make sure it's an operation we support
		if rest is not None:
			request.setResponseCode(500)
			return cjson.encode({"error": "smartproxy got a " + request.method + " to " + request.uri + ". don't know how to handle it"})+"\n"

		# farm it out.  generate a list of resources to PUT
		shards = self.conf_data.shards(db_name)
		nodes = set() # ensures once-per-node when replicas cohabitate
		for shard in shards:
			if rest:
				nodes.update(['/'.join([db, rest]) for db in self.conf_data.nodes(shard)])
			else:
				nodes.update(self.conf_data.nodes(shard))

		deferred = defer.Deferred()
		deferred.addCallback(make_success_callback(request))
		deferred.addErrback(make_errback(request))

		f = DbFetcher(self.conf_data, nodes, deferred, request.method, self.client_queue)
		f.fetch(request)
		return server.NOT_DONE_YET
	
	def get_db(self, request):
		"""Get general information about a database."""

		# chop off the leading /
		db_name = request.uri.strip('/')

		# fold function to reduce the sharded results
		def fold_results_fun(acc, result):
			result, idx = result                   #packed by DeferredList
			result, rep_id, factory = result       #packed by getPageFromAny
			shard, rep_idx = rep_id
			shard_idx = self.conf_data.get_index_from_shard(shard)
			node_idx = self.conf_data.shardmap[shard_idx][rep_idx]
			result = cjson.decode(result)
			acc['doc_count'] += result['doc_count']
			acc['doc_del_count'] += result['doc_del_count']
			acc['update_seq'][str(shard_idx)] = {str(node_idx): result['update_seq']}
			acc['purge_seq'][str(shard_idx)] = {str(node_idx): result['purge_seq']}
			acc['compact_running'].append(result['compact_running'])
			acc['disk_size'] += result['disk_size']
			return acc

		# success callback
		def finish_request(results):
			# results looks like (True, result) since we get here only if all succeeed
			# reduce over these results with fold_results_fun to produce output
			output = reduce(fold_results_fun,
					itertools.izip(itertools.imap(lambda x: x[1], results), # pull out result
						       itertools.count()),
					{'db_name': db_name,
					 'doc_count': 0,
					 'doc_del_count': 0,
					 'update_seq': {},
					 'purge_seq': {},
					 'compact_running': [],
					 'disk_size': 0})
			# encode the sequence information
			output['update_seq'] = changes.encode_seq(output['update_seq'])
			output['purge_seq'] = changes.encode_seq(output['purge_seq'])
			request.write(cjson.encode(output) + '\n')
			request.finish()
		
		# construct a DeferredList of the deferred sub-requests
		# fetches shard results from any replica of each shard
		# if any shard fails completely the whole thing fails fast
		deferred = defer.DeferredList(
			# map over all the shards and get a deferred that handles fail-over
			map(lambda shard: getPageFromAny(
				# create the upstream descriptions
				itertools.imap(
					lambda rep_idx, rep_uri:
					((shard, rep_idx),    # upstream identifier
					 rep_uri,             # url
					 [],                  # factory args
					 {}),                 # factor kwargs
					*zip(*enumerate(self.conf_data.nodes(shard))))),
				self.conf_data.shards(db_name)),
			fireOnOneErrback=1,
			consumeErrors=1)
		deferred.addCallback(finish_request)
		deferred.addErrback(make_errback(request))

		return server.NOT_DONE_YET

	def _rewrite_url(self, url):
		parts = url[7:].split('/')
		hostport = parts[0]
		host,port = hostport.split(':')

		new_url = url

		#  old:  http://host:5984/db/_view/designname/viewname
		#  new:  http://host:5984/db/_design/designname/_view/viewname
		if len(parts) > 2 and parts[2] == '_view':
			new_parts = [hostport, parts[1], '_design', parts[3], '_view', parts[4]]
			new_url = '/'.join(new_parts)	
			new_url = 'http://' + new_url

		if '_design%2F' in new_url:
			new_url = new_url.replace('_design%2F', '_design/')

		log.debug('_rewrite_url: "%s" => "%s"' % (url, new_url))
		return new_url

# vi: noexpandtab ts=2 sw=2 sts=2
