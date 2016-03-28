#!/usr/bin/env python
#-*- coding:utf-8 -*-

#
#  @author quqiang E-mail: qiang@acrcloud.com
#  @version 3.0.0
#  @create 2015.12.7
#

import sys
reload(sys)
sys.setdefaultencoding("utf8")
import traceback
from twisted.internet import reactor
from acrcloud_mana import acrcloudMana
from acrcloud_config import config
from twisted.internet.protocol import Protocol, ServerFactory

import platform
_platform = platform.system().lower()
if _platform == 'windows':
    from twisted.internet import iocpreactor
    try:
        #http://sourceforge.net/projects/pywin32/
        iocpreactor.install()
    except:
        pass
elif _platform == 'darwin':
    from twisted.internet import selectreactor
    try:
        selectreactor.install()
    except:
        pass
else:
    from twisted.internet import epollreactor
    try:
        epollreactor.install()
    except:
        pass


class Server(Protocol):

    def __init__(self):
        self.mana = acrcloudMana
        
    def connectionMade(self):
        self.mana.addClient(self)

    def connectionLost(self, reason):
        self.mana.delClient(self)

    def dataReceived(self, data):
        ret = self.mana.recData(data)
        self.sendData(ret)
        if ret[:5] == 'VALUE':
            self.sendData('END')

    def sendData(self, data):
        self.transport.write(data + '\r\n')

        
class ServerFactory(ServerFactory):

    def __init__(self):
        pass
    
    def buildProtocol(self, addr):
        return Server()
    
def acrcloud_monitor_service():
    try:
        port = config['server']['port']
        reactor.listenTCP(port, ServerFactory())
        reactor.run()
    except Exception as e:
        traceback.print_exc()

if __name__ == '__main__':
    acrcloud_monitor_service()
