import sys
import traceback
from twisted.internet import reactor
from acrcloud_monitor_main import acrcloudMana
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
        self.sendData(ret.encode())
        if ret[:5] == 'VALUE':
            self.sendData(b'END')

    def sendData(self, data):
        self.transport.write(data + b'\r\n')

        
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
