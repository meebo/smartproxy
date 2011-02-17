#!/usr/bin/python

import logging
import os
import pycurl
import cjson
import StringIO
import urllib
import urllib2
import base64
import zlib
import textwrap

from unittest import TestCase, main

from couchstub import CouchStub
import process

class Response:
	def __init__(self, code, body, headers):
		self.code = code
		self.body = cjson.decode(body)
		self.headers = headers

def req(url, method, body=None, headers=None):
	curl = pycurl.Curl()
	curl.setopt(pycurl.URL, url)
	outbuf = StringIO.StringIO()
	curl.setopt(pycurl.WRITEFUNCTION, outbuf.write)

	if body is not None:
		body = cjson.encode(body)
		if method=='POST':
			curl.setopt(pycurl.POSTFIELDS, body)
		else:
			inbuf = StringIO.StringIO(body)
			curl.setopt(pycurl.INFILESIZE, len(body))
			curl.setopt(pycurl.READFUNCTION, inbuf.read)
		curl.setopt(pycurl.HTTPHEADER, ['Content-type: application/json'])
	
	# prevent tests from hanging if the smartproxy hangs
	curl.setopt(pycurl.TIMEOUT, 5)

	if method=='PUT':
		curl.setopt(pycurl.UPLOAD, 1)
	elif method=='POST':
		curl.setopt(pycurl.POST, 1)
	elif method=='DELETE':
		curl.setopt(pycurl.CUSTOMREQUEST, 'DELETE')
	
	headers = {}
	def parse_header(txt):
		if ': ' in txt:
			k,v = txt.strip().split(': ',1)
			headers[k.lower()] = v
	curl.setopt(pycurl.HEADERFUNCTION, parse_header)

	curl.perform()
	rv = outbuf.getvalue()
	return Response(curl.getinfo(pycurl.HTTP_CODE), outbuf.getvalue(), headers)

def get(url, body=None, headers=None):
	return req(url, "GET", body, headers)

def put(url, body=None, headers=None):
	return req(url, "PUT", body, headers)

def post(url, body=None, headers=None):
	return req(url, "POST", body, headers)

def assert_raises(exception, function, *args, **kwargs):
	"""Make sure that when you call function, it raises exception"""
	try:
		function(*args, **kwargs)
		assert False, "Should have raised %s" % exception
	except exception:
		pass

def encode_seq(seq):
	def fold_list_seq_to_test_shard_seq(x, y):
		i, x = x
		y = y
		x.update({str(i):{str(i):y}})
		return (i+1, x)

	if isinstance(seq, list):
		l, seq = reduce(fold_list_seq_to_test_shard_seq, seq, (0, {}))

	return base64.urlsafe_b64encode(
	    zlib.compress(cjson.encode(seq), 1))

def decode_seq(seq):
	return cjson.decode(
	    zlib.decompress(
		base64.urlsafe_b64decode(seq)))

class ProxyTest(TestCase):
	def setUp(self):
		self.smartproxy_pid = process.background("/usr/bin/twistd -l log/smartproxy.log -n -y fixtures/smartproxy.tac")
		assert process.wait_for_connect("localhost", 22008), "Smartproxy didn't start"

	def tearDown(self):
		process.stop_pid(self.smartproxy_pid)
		process.wait_for_process_exit(self.smartproxy_pid)
		try:
			os.unlink("twistd.pid")
		except OSError:
			pass

	def testNothing(self):
		"""Trivial smartproxy health check"""
		assert urllib2.urlopen("http://localhost:22008/ruok").read()=="imok"

	def testGetDB(self):
		"""Try to GET information on a database.

		smartproxy should get info on each shard and merge them together
		"""
		be1 = CouchStub()
		be1.expect_GET("/test0").reply(200, dict(
			db_name="test0", 
			doc_count=5, 
			doc_del_count=1,
			update_seq=8,
			purge_seq=0,
			compact_running=False,
			disk_size=16384,
			instance_start_time="1250979728236424"))
		be1.listen("localhost", 23456)

		be2 = CouchStub()
		be2.expect_GET("/test1").reply(200, dict(
			db_name="test1", 
			doc_count=10, 
			doc_del_count=2,
			update_seq=16,
			purge_seq=0,
			compact_running=False,
			disk_size=16384,
			instance_start_time="1250979728236424"))
		be2.listen("localhost", 34567)

		resp = get("http://localhost:22008/test")

		be1.verify()
		be2.verify()

		self.assertEqual(resp.code, 200)
		self.assertEqual(resp.body['db_name'], 'test')
		self.assertEqual(resp.body['doc_count'], 15)
		self.assertEqual(resp.body['doc_del_count'], 3)
		self.assertEqual(resp.body['disk_size'], 32768)
		self.assertEqual(resp.body['update_seq'], encode_seq([8,16]))

	def testGetMissingDB(self):
		"""Try to GET information on a missing database."""
		be1 = CouchStub()
		be1.expect_GET("/test0").reply(404, dict(error="not_found",reason="missing"))
		be1.listen("localhost", 23456)

		be2 = CouchStub()
		be2.expect_GET("/test1").reply(404, dict(error="not_found",reason="missing"))
		be2.listen("localhost", 34567)

		resp = get("http://localhost:22008/test")

		be1.verify()
		be2.verify()

		self.assertEqual(resp.code, 404)
		self.assertEqual(resp.body['error'], 'not_found')
		self.assertEqual(resp.body['reason'], 'missing')

	def testPutDesign(self):
		"""Try to create a design document.
		
		smartproxy should redirect the request to the first shard.
		"""
		be1 = CouchStub()
		be1.expect_PUT("/funstuff0/_design/monkeys").reply(201, dict(
			ok=True, id="_design/monkeys", rev="1-2323232323"))
		be1.listen("localhost", 23456)

		be2 = CouchStub()
		be2.listen("localhost", 34567)

		resp = put("http://localhost:22008/funstuff/_design/monkeys", 
			{"views": {}})

		be1.verify()
		be2.verify()

		self.assertEqual(resp.code, 201)
		self.assertEqual(resp.body['ok'], True)
		self.assertEqual(resp.body['rev'], '1-2323232323')
	
	def testChanges(self):
		"""Query _changes on a db.

		smartproxy should send a _changes req to each shard and merge them.
		"""
		be1 = CouchStub()
		be1.expect_GET("/funstuff0/_changes?since=5").reply(200,
			textwrap.dedent(
			'''\
			{"results":[
			{"seq": 6, "id": "mywallet", "changes":[{"rev": "1-2345"}]},
			{"seq": 7, "id": "elsegundo", "changes":[{"rev": "2-3456"}]}
			],
			"last_seq"=7}'''),
			headers={"Content-Type": "text/plain;charset=utf8"},
			raw_body=True)
		be1.listen("localhost", 23456)

		be2 = CouchStub()
		be2.expect_GET("/funstuff1/_changes?since=12").reply(200,
			textwrap.dedent('''\
			{"results":[
			{"seq": 13, "id": "gottagetit", "changes":[{"rev": "1-2345"}]},
			{"seq": 14, "id": "gotgottogetit", "changes":[{"rev": "2-3456"}]}
			],
			"last_seq"=14}'''),
			headers={"Content-Type": "text/plain;charset=utf8"},
			raw_body=True)
		be2.listen("localhost", 34567)

		resp = get("http://localhost:22008/funstuff/_changes?since=%s" % encode_seq([5,12]))

		be1.verify()
		be2.verify()

		self.assertEqual(resp.code, 200)
		self.assertEqual(resp.headers["content-type"], "text/plain;charset=utf8")
		assert 'results' in resp.body
		res = resp.body['results']
		self.assertEqual(len(res), 4, "Should have 4 changes")

		# check that the sequence vectors increment correctly
		# the order the rows arrive is non-deterministic
		seq = [5,12]
		for row in res:
			if row["id"] in ["mywallet", "elsegundo"]:
				seq[0] += 1
			elif row["id"] in ["gottagetit", "gotgottogetit"]:
				seq[1] += 1
			else:
				assert False, "Got unexpected row %s" % row["id"]
			self.assertEqual(encode_seq(seq), row["seq"])
		self.assertEqual(encode_seq([7,14]), resp.body['last_seq'])

	def testTempView(self):
		"""Make a temp view."""
		be1 = CouchStub()
		be1.expect_POST("/funstuff0/_temp_view").reply(200, dict(
			total_rows=2,
			offset=0,
			rows=[
				{"id":"a", "key":"a", "value": "b"},
				{"id":"c", "key":"c", "value": "d"}
			]))
		be1.listen("localhost", 23456)

		be2 = CouchStub()
		be2.expect_POST("/funstuff1/_temp_view").reply(200, dict(
			total_rows=2,
			offset=0,
			rows=[
				{"id":"x", "key":"b", "value": "c"},
				{"id":"y", "key":"d", "value": "e"}
			]))
		be2.listen("localhost", 34567)

		resp = post("http://localhost:22008/funstuff/_temp_view", body={"language":"javascript", "map": "function(doc) { emit(doc.x, doc.y); }"})

		be1.verify()
		be2.verify()

		self.assertEqual(resp.body["total_rows"], 4)
		self.assertEqual(resp.body["offset"], 0)
		self.assertEqual(len(resp.body["rows"]), 4)
		self.assertEqual(resp.body["rows"][0]["key"], "a")
		self.assertEqual(resp.body["rows"][1]["key"], "b")
		self.assertEqual(resp.body["rows"][2]["key"], "c")
		self.assertEqual(resp.body["rows"][3]["key"], "d")

	def testMultikeyGet(self):
		"""Make a temp view."""
		be1 = CouchStub()
		be1_request = be1.expect_POST("/funstuff0/_all_docs?include_docs=true")
		be1_request.reply(200, dict(
			total_rows=2,
			offset=0,
			rows=[
				{"id":"a", "key":"a", "value": {"rev":"2"},"doc":"b"},
				{"id":"c", "key":"c", "value": {"rev":"3"},"doc":"d"}
			]))
		be1.listen("localhost", 23456)

		be2 = CouchStub()
		be2_request = be2.expect_POST("/funstuff1/_all_docs?include_docs=true")
		be2_request.reply(200, dict(
			total_rows=2,
			offset=0,
			rows=[
				{"id":"b", "key":"b", "value": {"rev":"7"},"doc":"z"},
				{"id":"y", "key":"y", "value": {"rev":"9"},"doc":"w"}
			]))
		be2.listen("localhost", 34567)

		resp = post("http://localhost:22008/funstuff/_all_docs?include_docs=true", body={"keys":["a","c","x","y"]})

		be1.verify()
		be2.verify()
		
		be1_post = cjson.decode(be1_request.input_body)
		be2_post = cjson.decode(be2_request.input_body)

		def lounge_hash(x):
			crc = zlib.crc32(x,0)
			return (crc >> 16)&0x7fff

		keys1 = be1_post['keys']
		keys2 = be2_post['keys']
		keys = {0:keys1, 1:keys2}
		num_shards = 2
		for v, k in keys.items():
			for key in k:
				self.assertEqual(lounge_hash(key) % num_shards, int(v))

		self.assertEqual(resp.body["total_rows"], 4)
		self.assertEqual(resp.body["offset"], 0)
		self.assertEqual(len(resp.body["rows"]), 4)
		rows = [x["key"] for x in resp.body["rows"]]
		rows.sort()
		self.assertEqual(rows, ["a","b","c","y"])

	def testReverse(self):
		"""Query a view with descending=true"""
		be1 = CouchStub()
		be1.expect_GET("/funstuff0/_design/fun").reply(200, dict(
			views=dict(
				stuff=dict(
					map="function(doc){}"
				)
			)))
		be1.expect_GET("/funstuff0/_design/fun/_view/stuff?descending=true").reply(200, dict(
			total_rows=2,
			offset=0,
			rows=[
				{"id":"a", "key":"2", "value": "a"},
				{"id":"c", "key":"1", "value": "b"}
			]))
		be1.listen("localhost", 23456)

		be2 = CouchStub()
		be2.expect_GET("/funstuff1/_design/fun/_view/stuff?descending=true").reply(200, dict(
			total_rows=2,
			offset=0,
			rows=[
				{"id":"x", "key":"3", "value": "c"},
				{"id":"y", "key":"0", "value": "e"}
			]))
		be2.listen("localhost", 34567)

		resp = get("http://localhost:22008/funstuff/_design/fun/_view/stuff?descending=true")

		be1.verify()
		be2.verify()

		self.assertEqual(resp.body["total_rows"], 4)
		self.assertEqual(resp.body["offset"], 0)
		self.assertEqual(len(resp.body["rows"]), 4)
		self.assertEqual(resp.body["rows"][0]["key"], "3")
		self.assertEqual(resp.body["rows"][1]["key"], "2")
		self.assertEqual(resp.body["rows"][2]["key"], "1")
		self.assertEqual(resp.body["rows"][3]["key"], "0")
	
	def testBulkDocs(self):
		be1 = CouchStub()
		be1_request = be1.expect_POST("/funstuff0/_bulk_docs")
		be1_request.reply(201, [
			{"id":"b","rev":"1-23456"},
			{"id":"e","error":"conflict","reason":"Document update conflict."}
		])
		be1.listen("localhost", 23456)

		be2 = CouchStub()
		be2_request = be2.expect_POST("/funstuff1/_bulk_docs")
		be2_request.reply(201, [
			{"id":"a","rev":"1-23456"},
			{"id":"c","rev":"2-34567"}
		])
		be2.listen("localhost", 34567)

		resp = post("http://localhost:22008/funstuff/_bulk_docs", {"docs":[
			{"_id":"a","how":"low"},
			{"_id":"b","can":"a"},
			{"_id":"c","punk":"get"},
			{"_id":"e","bannedin":"dc"}
		]})

		be1.verify()
		be2.verify()

		be1_post = cjson.decode(be1_request.input_body)
		be2_post = cjson.decode(be2_request.input_body)

		self.assertEqual(len(resp.body), 4)
		for row in resp.body:
			if row['id']=='e':
				self.assertEqual(row['error'],'conflict')
			else:
				assert "rev" in row

		be1_post = cjson.decode(be1_request.input_body)
		self.assertEqual(len(be1_post['docs']), 2)
		self.assertEqual(sorted([row['_id'] for row in be1_post['docs']]), ['b', 'e'])

		be2_post = cjson.decode(be2_request.input_body)
		self.assertEqual(len(be2_post['docs']), 2)
		self.assertEqual(sorted([row['_id'] for row in be2_post['docs']]), ['a', 'c'])

	def testBulkDocsWrongContentType(self):
		"""Make a bulk_docs req with the wrong content-type.

		We should handle this the same way a single node does (treat
		the body as JSON)
		"""
		be1 = CouchStub()
		be1_request = be1.expect_POST("/funstuff0/_bulk_docs")
		be1_request.reply(201, [
			{"id":"b","rev":"1-23456"},
			{"id":"e","error":"conflict","reason":"Document update conflict."}
		])
		be1.listen("localhost", 23456)

		be2 = CouchStub()
		be2_request = be2.expect_POST("/funstuff1/_bulk_docs")
		be2_request.reply(201, [
			{"id":"a","rev":"1-23456"},
			{"id":"c","rev":"2-34567"}
		])
		be2.listen("localhost", 34567)

		resp = post("http://localhost:22008/funstuff/_bulk_docs", {"docs":[
			{"_id":"a","how":"low"},
			{"_id":"b","can":"a"},
			{"_id":"c","punk":"get"},
			{"_id":"e","bannedin":"dc"}
		]}, {"Content-Type": "application/x-www-form-urlencoded"})

		be1.verify()
		be2.verify()

		be1_post = cjson.decode(be1_request.input_body)
		be2_post = cjson.decode(be2_request.input_body)

		self.assertEqual(len(resp.body), 4)
		for row in resp.body:
			if row['id']=='e':
				self.assertEqual(row['error'],'conflict')
			else:
				assert "rev" in row

		be1_post = cjson.decode(be1_request.input_body)
		self.assertEqual(len(be1_post['docs']), 2)
		self.assertEqual(sorted([row['_id'] for row in be1_post['docs']]), ['b', 'e'])

		be2_post = cjson.decode(be2_request.input_body)
		self.assertEqual(len(be2_post['docs']), 2)
		self.assertEqual(sorted([row['_id'] for row in be2_post['docs']]), ['a', 'c'])

if __name__=="__main__":
	if os.environ.get("DEBUG",False):
		console = logging.StreamHandler()
		console.setLevel(logging.DEBUG)
		logging.basicConfig(level=logging.DEBUG, handler=console)
	main()
# vi: noexpandtab ts=2 sts=2 sw=2
