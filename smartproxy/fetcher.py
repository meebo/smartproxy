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
import cPickle
import lounge
import itertools
import os
import random
import re
import sys
import time
import urllib
import urllib2
import zlib

import cjson

from twisted.python import log
from twisted.internet import defer
from twisted.internet import protocol, reactor, defer, process, task, threads
from twisted.internet.error import ConnectError
from twisted.protocols import basic
from twisted.web import server, resource, client
from twisted.python.failure import DefaultException
from twisted.web.client import _makeGetterFactory

from reducer import Reducer

import proxy

def getPageWithHeaders(url, *args, **kwargs):
	# basically a clone of client.getPage, but with a handle on the factory
	# so we can pull the headers later
	scheme, host, port, path = client._parse(url)
	factory = client.HTTPClientFactory(url, *args, **kwargs)
	reactor.connectTCP(host, port, factory)
	return factory

def getPageFromAny(upstreams, factory=client.HTTPClientFactory,
		   context_factory=None):
	if not upstreams:
		raise error.Error(http.INTERNAL_SERVER_ERROR)

	def subgen():
		lastError = None
		for (identifier, url, args, kwargs) in upstreams:
			subfactory = client._makeGetterFactory(url,
							       factory,
							       context_factory,
							       *args, **kwargs)
			wait = defer.waitForDeferred(subfactory.deferred)
			yield wait
			try:
				yield (wait.getResult(), identifier, subfactory)
				return
			except ConnectError:
				lastError = sys.exc_info()[1]
		raise lastError and lastError or error.Error(http.INTERNAL_SERVER_ERROR)
	return defer.deferredGenerator(subgen)()

def getPageFromAll(upstreams, factory=client.HTTPClientFactory,
		   context_factory=None):
	def makeUpstreamGetter(upstream):
		identifier, url, args, kwargs = upstream
		subfactory = client._makeGetterFactory(url,
						       factory,
						       context_factory,
						       *args, **kwargs)
		subfactory.deferred.addCallback(lambda x: (x, identifier, subfactory))
		return subfactory.deferred

	return map(makeUpstreamGetter, upstreams)

def prep_backend_headers(hed, cfg):
	# rewrite the Location to be a proxied url
	to_remove = []
	for k in hed:
		if k.lower()=='location':
			# http://localhost/db5 -> http://localhost/db
			url = hed[k][0]
			scheme, netloc, path, params, query, fragment = urllib2.urlparse.urlparse(url)
			pieces = path.split('/')
			if len(pieces)>0 and pieces[1]:
				pieces[1] = cfg.get_db_from_shard(pieces[1])
			path = '/'.join(pieces)
			hed[k] = [urllib2.urlparse.urlunparse((scheme, netloc, path, params, query, fragment))]
		elif k.lower()=='content-length':
			to_remove.append(k)
	for k in to_remove:
		hed.pop(k)
	return hed

class HttpFetcher:
	def __init__(self, name, nodes, deferred, client_queue):
		self._name = name
		self._remaining_nodes = nodes
		self._deferred = deferred
		self.client_queue = client_queue

	def fetch(self, request=None):
		self._headers = request and request.getAllHeaders() or {}
		self.next()
	
	def next(self):
		url = self._remaining_nodes[0]
		self._remaining_nodes = self._remaining_nodes[1:]
		self.client_queue.enqueue(url, self._onsuccess, self._onerror, self._headers)

	def _onsuccess(self, result, *args, **kwargs):
		pass

	def _onerror(self, data):
		log.msg("Unable to fetch data from node %s" % data)
		if len(self._remaining_nodes) == 0:
			log.msg("unable to fetch data from shard %s.  Failing" % self._name)
			self._deferred.errback(data)
		else:
			self.next()

class UuidFetcher(HttpFetcher):
	def __init__(self, db, urls, deferred, body, conf, client_queue):
		HttpFetcher.__init__(self, "uuids", urls, deferred, client_queue)
		self._db = db
		if self._db.endswith('/'):
			self._db = self._db[:-1]
		self._body = body
		self._config = conf
	
	def _onsuccess(self, page):
		uuid = cjson.decode(page)["uuids"][0]

		self.put_doc(uuid)
	
	def put_doc(self, id):
		# identify the shard for this uuid
		shards = self._config.shards(self._db)
		shard_idx = proxy.which_shard(proxy.lounge_hash(id), len(shards))

		# generate upstream tuples for getPageFromAny
		shard_uris = itertools.imap(
			lambda idx, uri: (
				idx,                 # replica identifier
				'/'.join([uri, id]), # shard uri
				[],                  # args
				{ 'method' : 'PUT',  # kwargs
				  'postdata' : self._body
				}),
			itertools.count(),
			self._config.nodes(shards[shard_idx]))

		# set up a callback to unpack
		def succeed(result):
			data, identifier, factory = result
			self._deferred.callback(
				(int(factory.status),
				 prep_backend_headers(factory.response_headers, self._config),
				 data))

		getPageFromAny(shard_uris).addCallbacks(succeed, self._deferred.errback)


class MapResultFetcher(HttpFetcher):
	def __init__(self, shard, nodes, reducer, deferred, client_queue, body=None):
		HttpFetcher.__init__(self, shard, nodes, deferred, client_queue)
		self._body = body
		self._reducer = reducer

	def _onsuccess(self, page):
		self._reducer.process_map(page, self._name, self.factory.response_headers, int(self.factory.status))

	def _onerror(self, data, request):
		log.msg("Unable to fetch data from node %s" % data)
		if len(self._remaining_nodes) == 0:
			log.msg("unable to fetch data from shard %s.  Failing" % self._name)
			self._deferred.errback(data)
		else:
			self.fetch(request)

	def fetch(self, request=None):
		url = self._remaining_nodes[0]
		self._headers = request and request.getAllHeaders() or {}
		method = request and request.method or 'GET'

		self._remaining_nodes = self._remaining_nodes[1:]
		if method=='POST':
			if self._body:
				body = self._body
			else:
				body = request and request.content.read() or ''
			self.factory = getPageWithHeaders(url=url, postdata=body, method=method, headers=self._headers)
		else:
			self.factory = getPageWithHeaders(url=url, method=method, headers=self._headers)
		self.factory.deferred.addCallback(self._onsuccess)
		self.factory.deferred.addErrback(self._onerror, request=request)
		

class DbFetcher(HttpFetcher):
	"""Perform an HTTP request on all shards in a database."""
	def __init__(self, config, nodes, deferred, method, client_queue):
		self._method = method
		self._config = config
		HttpFetcher.__init__(self, config, nodes, deferred, client_queue)

	def fetch(self, request=None):
		self._remaining = len(self._remaining_nodes)
		self._failed = False
		headers = request and request.getAllHeaders() or {}
		for url in self._remaining_nodes:
			factory = getPageWithHeaders(url = url, method=self._method, headers=headers)
			deferred = factory.deferred
			deferred.addCallback(self._onsuccess, request=request, factory=factory)
			deferred.addErrback(self._onerror)
	
	def _onsuccess(self, data, request=None, factory=None):
		self._remaining -= 1
		if self._remaining < 1:
			# can't call the deferred twice
			if not self._failed:
				self._deferred.callback((int(factory.status), prep_backend_headers(factory.response_headers, self._config), data))

	def _onerror(self, data):
		# don't retry on our all-database operations
		if not self._failed:
			# prevent from calling the errback twice
			log.msg("unable to fetch from node %s; db operation %s failed" % (data, self._name))
			self._failed = True
			self._deferred.errback(data)

class ChangesFetcher(HttpFetcher):
	def __init__(self, shard, nodes, reducer, deferred, client_queue):
		HttpFetcher.__init__(self, shard, nodes, deferred, client_queue)
		self._reducer = reducer
		self._shard = shard

	def _onsuccess(self, page, *args, **kwargs):
		self._reducer.process_map(page, self._shard, self.factory.response_headers)

	def fetch(self, request=None):
		self._request = request
		url = self._remaining_nodes[0]
		headers = request and request.getAllHeaders() or {}
		self._remaining_nodes = self._remaining_nodes[1:]
		self.factory = getPageWithHeaders(url=url, method='GET', headers=headers)
		self.factory.deferred.addCallback(self._onsuccess)
		self.factory.deferred.addErrback(self._onerror)

	def next(self):
		self.fetch(self._request)

class DbGetter(DbFetcher):
	"""Get info about every shard of a database and accumulate the results."""
	def __init__(self, config, nodes, deferred, name, client_queue):
		DbFetcher.__init__(self, config, nodes, deferred, 'GET', client_queue)
		self._acc = {"db_name": name, "doc_count": 0, "doc_del_count": 0, "update_seq": len(nodes)*[0], "purge_seq": 0, "compact_running": False, "disk_size": 0,
			"compact_running_shards": [], # if compact is running, which shards?
			"update_seq_shards": {},      # aggregate update_seq isn't really relevant
			"purge_seq_shards": {},       # ditto purge_seq
			}
	
	def _onsuccess(self, data, *args, **kwargs):
		# accumulate results
		res = cjson.decode(data)
		self._acc["doc_count"] += res.get("doc_count",0)
		self._acc["doc_del_count"] += res.get("doc_del_count",0)
		self._acc["disk_size"] += res.get("disk_size",0)
		self._acc["compact_running"] = self._acc["compact_running"] or res.get("compact_running", False)
		if res.get("compact_running", False):
			self._acc["compact_running_shards"].append(res["db_name"])

		if "update_seq" in res:
			shard_idx = self._config.get_index_from_shard(res["db_name"])
			self._acc["update_seq"][shard_idx] = res["update_seq"]
	
		# this will be kinda meaningless...
		if "purge_seq" in res:
			self._acc["purge_seq_shards"][res["db_name"]] = res["purge_seq"]
			if res["purge_seq"] > self._acc["purge_seq"]:
				self._acc["purge_seq"] = res["purge_seq"]

		self._remaining -= 1
		if self._remaining < 1:
			if 'request' in kwargs and 'factory' in kwargs:
				kwargs['request'].headers.update(kwargs['factory'].response_headers)
				# remove length, send chunked response
				kwargs['request'].headers.pop('content-length', None)
			self._acc["update_seq"] = cjson.encode(self._acc["update_seq"])
			self._deferred.callback(self._acc)

class ViewFetcher(HttpFetcher):
	def __init__(self, config, nodes, database, uri, view, deferred, client_queue, reduce_queue):
		HttpFetcher.__init__(self, "reduce_func", nodes, deferred, client_queue)
		self._config = config
		self._view = view
		self._database = database
		self._uri = uri
		self._reduce_queue = reduce_queue
		self._client_queue = client_queue
		self._failed = False

	def fetch(self, request=None):
		self._request = request
		self._args = self._request and self._request.args or {}

		if 'false' not in self._args.get('reduce', ['true']):
			return HttpFetcher.fetch(self, self._request)
		# if reduce=false, then we don't have to pull the reduce func out
		# of the design doc.  Just go straight to the view
		return self._onsuccess("{}")

	def _onsuccess(self, page, *args, **kwargs):
		design_doc = cjson.decode(page)
		reduce_func = design_doc.get("views",{}).get(self._view, {}).get("reduce", None)
		if reduce_func is not None:
			reduce_func = reduce_func.replace("\n","")
		shards = self._config.shards(self._database)
		reducer = Reducer(reduce_func, len(shards), self._args, self._deferred, self._reduce_queue)

		# make sure we don't call this deferred twice (using self._failed)
		def handle_success(data):
			log.debug("ViewFetcher: handle_succes")
			if not self._failed:
				self._deferred.callback(data)

		def handle_error(data):
			if not self._failed:
				self._failed = True
				self._deferred.errback(data)

		for shard in shards:
			shard_deferred = defer.Deferred()
			shard_deferred.addCallback(handle_success)
			shard_deferred.addErrback(handle_error)

			nodes = self._config.nodes(shard)
			urls = ["/".join([node, self._uri]) for node in nodes]
			fetcher = MapResultFetcher(shard, urls, reducer, shard_deferred, self._client_queue)
			fetcher.fetch(self._request)

class AllDbFetcher(HttpFetcher):
	def __init__(self, config, nodes, deferred, client_queue):
		HttpFetcher.__init__(self, "_all_dbs", nodes, deferred, client_queue)
		self._config = config
	
	def _onsuccess(self, page, *args, **kwargs):
		# in is a list of shards
		# out is a list of db names
		shards = cjson.decode(page)
		dbs = dict([
			(urllib.unquote(self._config.get_db_from_shard(
				urllib.quote(shard, ''))), 1)
			for shard in shards])
		if 'factory' in kwargs and 'request' in kwargs:
			kwargs['request'].headers.update(kwargs['factory'].response_headers)
			# remove length, send chunked response
			kwargs['request'].headers.pop('content-length', None)
		self._deferred.callback(dbs.keys())

class ProxyFetcher(HttpFetcher):
	"""Pass along a GET, POST, or PUT."""
	def __init__(self, name, nodes, deferred, client_queue):
		HttpFetcher.__init__(self, name, nodes, deferred, client_queue)

	def fetch(self, request=None):
		url = self._remaining_nodes[0]
		qs = urllib.urlencode([(k,v) for k in request.args for v in request.args[k]] or '')
		if qs: url += '?' + qs
		headers = request and request.getAllHeaders() or {}
		method = request and request.method or 'GET'
		body = ''
		if method=='PUT' or method=='POST':
			body = request and request.content.read() or ''
		self._remaining_nodes = self._remaining_nodes[1:]
		self._remaining_nodes = []
		self.factory = getPageWithHeaders(url, method=method, postdata=body, headers=headers)
		self.factory.deferred.addCallback(self._onsuccess)
		self.factory.deferred.addErrback(self._onerror)

	def _onsuccess(self, page, *args, **kwargs):
		self._deferred.callback((int(self.factory.status), self.factory.response_headers, page))

# vi: noexpandtab ts=2 sts=2 sw=2
