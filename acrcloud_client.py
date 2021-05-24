#!/usr/bin/env python
#-*- coding:utf-8 -*-

import time
import json
import tools_memcache
from acrcloud_config import config

class MonitorClient:
    def __init__(self):
        self.mc = tools_memcache.Client(["127.0.0.1:{0}".format(config["server"]["port"])])

    def refresh(self):
        print self.mc.set('refresh', '')

    def stop(self):
        print self.mc.set('stop', '')

    def state(self, id):
        state = self.mc.get('state:'+str(id))
        jsonstate = json.loads(state)
        print jsonstate

    def start(self):
        while 1:
            cmd = raw_input("(1.refresh, 2.state, 3.stop): ")
            if cmd == '1':
                self.refresh()
            elif cmd == '2':
                id = raw_input('stream_id: ')
                self.state(id.strip())
            elif cmd == '3':
                self.stop()


if __name__ == '__main__':
    mc = MonitorClient()
    mc.start()
