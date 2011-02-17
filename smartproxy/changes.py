#Copyright 2009 Meebo, Inc.
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

import base64
import cjson
import zlib
import copy

from collections import deque

from twisted.python import log

import streaming

def encode_seq(seq):
	return base64.urlsafe_b64encode(
	    zlib.compress(cjson.encode(seq), 1))

def decode_seq(seq):
	return cjson.decode(
	    zlib.decompress(
		base64.urlsafe_b64decode(seq)))

def transformations(continuous=False):
	if not continuous:
		def _input(data):
			if data == '{"results":[':
				return
			if data == '],':
				return

			if data.startswith('"last_seq"'):
				data = '{' + data
			else:
				data = data.rstrip(',')
			return cjson.decode(data)

		def _output(data, hold_change=deque(['{"results":[\n'])):
			if 'last_seq' in data:
				data['last_seq'] = encode_seq(data['last_seq'])
				last_seq = cjson.encode(data)[1:]
				return '%s],\n%s\n' % (_output(hold_change.popleft()),
									   last_seq)
			hold_change.append(data)
			data = hold_change.popleft()
			if 'seq' in data:
				data['seq'] = encode_seq(data['seq'])
				if hold_change: # more coming
					return cjson.encode(data) + ',\n'
				else: # end (no comma)
					return cjson.encode(data) + '\n'
			return data

		return _input, _output
	else:
		def _output(data):
			if 'seq' in data:
				data['seq'] = encode_seq(data['seq'])
			elif 'last_seq' in data:
				data['last_seq'] = encode_seq(data['last_seq'])
			return cjson.encode(data) + '\n'

		return cjson.decode, _output

class ChangesProxy(streaming.MultiPCP):
	def __init__(self, consumer, since):
		streaming.MultiPCP.__init__(self, consumer)
		self.seq = copy.deepcopy(since)

	def write(self, channelData):
		channel, data = channelData
		if not data or not self.consumer:
			return	
		elif 'seq' in data:
			self.seq[channel] = data['seq']
			data['seq'] = copy.deepcopy(self.seq)
			self.consumer.write(data)
		else:
			# don't write here!
			# data could be an error message
			# if we write anything, we'll automatically give a 200 code
			# we need to fake the error response somewhere else
			pass

	def finish(self):
		if self.consumer is not None:
			self.consumer.write({'last_seq': self.seq})
		streaming.MultiPCP.finish(self)

# vi: noexpandtab ts=2 sts=2 sw=2
