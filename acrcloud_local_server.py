#!/usr/bin/env python
#-*- coding:utf-8 -*-

#
#  @author quqiang E-mail: qiang@acrcloud.com
#  @version 3.0.0
#  @create 2015.12.7
#

import sys
import traceback
from multiprocessing import freeze_support
from acrcloud_config import config
from twisted.internet import reactor
from acrcloud_manager import core_obj
from twisted.internet.protocol import Protocol, ServerFactory, Factory

freeze_support()

reload(sys)
sys.setdefaultencoding("utf8")

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


class A_Protocol(Protocol):

    def __init__(self, factory):
        self.mana = factory.core_object
       
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
       
class SFactory(ServerFactory):
    
    def __init__(self, core_object):
        self.core_object = core_object

    def buildProtocol(self, addr):
        return A_Protocol(self) 

def acrcloud_monitor_service():
    try:
        port = config['server']['port']
        core_object = core_obj()
        factory = SFactory(core_object)
        reactor.listenTCP(port, factory)
        reactor.run()
    except Exception as e:
        traceback.print_exc()

if __name__ == "__main__":
    acrcloud_monitor_service()
