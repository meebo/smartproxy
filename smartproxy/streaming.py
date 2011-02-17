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

import warnings

import cjson

from twisted.python import log
from twisted.web import error, http, client
from twisted.internet import interfaces
from twisted.protocols import pcp
from twisted.internet.error import ConnectionDone

from zope.interface import implements


class HTTPProducer(client.HTTPClientFactory):
	"""
	Like twisted.web.client.HTTPDownloader except instead of streaming
	to a file I stream to a consumer.
	"""
	
	protocol = client.HTTPPageDownloader

	def __init__(self, url, consumer, *args, **kwargs):
		client.HTTPClientFactory.__init__(self, url, *args, **kwargs)
		self.consumer = consumer

	def buildProtocol(self, addr):
		p = client.HTTPClientFactory.buildProtocol(self, addr)
		self.consumer.registerProducer(p, True)
		return p

	def pageStart(self, partialContent):
		pass

	def pagePart(self, data):
		self.consumer.write(data)

	def pageEnd(self):
		self.consumer.unregisterProducer()
		self.consumer.finish()

class HTTPLineProducer(HTTPProducer):
	def __init__(self, *args, **kwargs):
		HTTPProducer.__init__(self, *args, **kwargs)
		self.oldLineReceived = None

	def buildProtocol(self, addr):
		p = HTTPProducer.buildProtocol(self, addr)
		p.setRawMode = lambda: self.setRawModeWrapper(p)
		p.setLineMode = lambda r='': self.setLineModeWrapper(p, r)
		return p

	def setRawModeWrapper(self, protocol):
		if self.oldLineReceived:
			protocol.setRawMode()
			return
		self.oldLineReceived = protocol.lineReceived
		self.oldDelimiter = protocol.delimiter
		protocol.lineReceived = self.gotLine
		protocol.delimiter = '\n'

	def setLineModeWrapper(self, protocol, rest=''):
		if not self.oldLineReceived:
			self.protocol.setLineMode(rest)
			return
		protocol.lineReceived = self.oldLineReceived
		protocol.delimiter = self.oldDelimiter
		protocol.dataReceived(rest)
	
	def gotLine(self, data):
		if data:
			self.pagePart(data)

class MultiPCP(pcp.BasicProducerConsumerProxy):
	class MultiPCPChannel:
		implements(interfaces.IProducer, interfaces.IConsumer)

		def __init__(self, name, sink):
			self.name = name
			self.sink = sink
			self.producer = None

		# Producer methods

		def pauseProducing(self):
			self.producer.pauseProducing()

		def resumeProducing(self):
			self.producer.resumeProducing()

		def stopProducing(self):
			if self.producer is not None:
				self.producer.stopProducing()

		# Consumer methods

		def write(self, data):
			self.sink.write((self.name, data))

		def finish(self):
			pass
			#self.sink.deleteChannel(self.name)

		def registerProducer(self, producer, streaming):
			self.producer = producer

		def unregisterProducer(self):
			if self.producer is not None:
				self.producer = None
				self.sink.deleteChannel(self.name)

		def __repr__(self):
			return '<%s@%x around %s>' % (self.__class__, id(self), self.sink)


	def __init__(self, consumer):
		pcp.BasicProducerConsumerProxy.__init__(self, consumer)
		self.channels = {}

	def createChannel(self, name):
		if name in self.channels:
			raise ValueError, "channel already open"
		
		self.channels[name] = self.MultiPCPChannel(name, self)
		return self.channels[name]

	def deleteChannel(self, name):
		del self.channels[name]
		if not self.channels and self.consumer:
			self.consumer.unregisterProducer()

	def pauseProducing(self):
		for channel in self.channels.itervalues():
			channel.pauseProducing()

	def resumeProducing(self):
		for channel in self.channels.itervalues():
			channel.resumeProducing()

	def stopProducing(self):
		for channel in self.channels.itervalues():
			channel.stopProducing()

	def write(self, channelData):
		## Override to specify how this proxy merges channels ##
		channel, data = channelData
		pcp.BasicProducerConsumerProxy.write(self, data)

	def finish(self):
		if self.consumer is not None:
			self.consumer.unregisterProducer()
			self.consumer.finish()
			self.consumer = None

	def registerProducer(self, producer, streaming):
		warnings.warn("directly registering producers with MultiPCP objects is not supported, use createChannel() instead", category=RuntimeWarning)

	def unregisterProducer(self):
		warnings.warn("directly unregistering producers with MultiPCP objects is not supported, use deleteChannel() instead", category=RuntimeWarning)

class LinePCP(pcp.BasicProducerConsumerProxy):
	def __init__(self, consumer, xform=lambda x: x + '\n'):
		pcp.BasicProducerConsumerProxy.__init__(self, consumer)
		self.xform = xform

	def write(self, data):
		result = self.xform(data)
		if result:
			pcp.BasicProducerConsumerProxy.write(self, result)

	def finish(self):
		if self.consumer is not None:
			self.consumer.unregisterProducer()
			self.consumer.finish()
			self.consumer = None


# vi: noexpandtab ts=4 sts=4 sw=4
