#!/usr/bin/env python
#-*- coding:utf-8 -*-
import time
import json
import memcache

info = {'access_key':'48ba195edd0107061f2062f0cd2bf5a3',
        'access_secret':'KxTGNBiZOM0nS2TmY22ApFjsHeYGjnVyYm9Bh5Uc',
        'rec_host':'ap-southeast-1.api.acrcloud.com',
        'stream_id':None,
        'stream_url':None,
        'interval':8,
        'monitor_length':12,
        'monitor_timeout':25,
        'rec_timeout':5,
        'record':0,
        'record_before':3,
        'record_after':20,
        'delay':1,
        'filter_chinese':1,
        'ischeck':False
}


class MonitorClient:
    def __init__(self):
        self.mc = memcache.Client(["127.0.0.1:3005"])

    def refresh(self):
        print self.mc.set('refresh', '')

    def restart(self, id):
        print self.mc.set('restart', json.dumps({'stream_id':str(id), 'stream_url':''}))

    def pause(self, id):
	print self.mc.set('pause', json.dumps({'stream_id':str(id), 'stream_url':''}))

    def state(self, id):
	print 'your input: ', id
        state = self.mc.get('state-'+str(id))
        jsonstate = json.loads(state)
        if id == 'all':
            for item in jsonstate.get('response', []):
                print item
                print '~'*20
        else:
            print jsonstate.get('response', '')
        
    def start(self):
        while 1:
            cmd = raw_input("(1.refresh, 2.restart, 3.state, 4.pause): ")
            if cmd == '1':
                self.refresh()
            elif cmd == '2':
                id = raw_input('stream_id: ')
                self.restart(id.strip())
            elif cmd == '3':
                id = raw_input('stream_id: ')
                self.state(id.strip())
            elif cmd == '4':
                id = raw_input('stream_id: ')
                self.pause(id.strip())
            else:
                pass
        

if __name__ == '__main__':
    mc = MonitorClient()
    mc.start()
