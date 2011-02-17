import os

#import sys 
#sys.path.insert(0, '')

from twisted.application import internet, service
from twisted.web import static, server, script
from twisted.python import log

from lounge.prefs import Prefs
from smartproxy.proxy import SmartproxyResource

prefs = Prefs(os.environ.get("PREFS",'/etc/lounge/smartproxy.xml'), no_missing_keys=True)

http_port = prefs.get_pref("/http_port")

loglevel = prefs.get_pref('/log_level')
if loglevel=='DEBUG':
	log.debug = log.msg
else:
	log.debug = lambda x: None

application = service.Application('smartproxy')

site = server.Site(SmartproxyResource(prefs))
sc = service.IServiceCollection(application)
i = internet.TCPServer(http_port, site, interface="0.0.0.0")
i.setServiceParent(sc)

# vi: noexpandtab ts=2 sts=2 sw=2
