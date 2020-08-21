#!/usr/bin/env python
#-*- coding:utf-8 -*-

import os
import sys
import json
import time
import Queue
import socket
import random
import signal
import logging
import datetime
import requests
import traceback
from dateutil.relativedelta import *

from acrcloud_logger import AcrcloudLogger

reload(sys)
sys.setdefaultencoding("utf8")

socket.setdefaulttimeout(60)

USER_AGENTS = [
        'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11',
        'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12',
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X; en) AppleWebKit/419 (KHTML, like Gecko) Safari/419.3',
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.83 Safari/537.1',
        'Mozilla/5.0 (Windows NT 6.1; rv:14.0) Gecko/20100101 Firefox/14.0.1'
]

class Acrcloud_State:

    def __init__(self, stateQueue, shareDict, config):
        self.stateQueue = stateQueue
        self.shareDict = shareDict
        self.config = config
        self.access_key = config['user']['access_key']
        self.state_callback_key = "state_callback_url_" + self.access_key
        self.timeout = 15
        self.state_history = {} #用于保存历史的流状态
        self.initLog()
        self.dlog.logger.warning("Init State Worker success")

    def initLog(self):
        self.dlog = AcrcloudLogger("StateWorker.log", logging.INFO)
        if not self.dlog.addFilehandler(logfile = "StateWorker.log", logdir = self.config['log']['dir']):
            sys.exit(1)
        if not self.dlog.addStreamHandler():
            sys.exit(1)

    def do_post(self, post_list):
        if self.state_callback_key in self.shareDict:
            state_callback_url = self.shareDict[self.state_callback_key]
        if not state_callback_url:
            return

        try:
            headers = {'content-type': 'application/json'}
            post_data = {"status": post_list}
            if state_callback_url.startswith("https"):
                response = requests.post(state_callback_url, data=json.dumps(post_data), headers=headers, verify=False, timeout=self.timeout)
            else:
                response = requests.post(state_callback_url, data=json.dumps(post_data), headers=headers, timeout=self.timeout)

            self.dlog.logger.warn("Warn@state_callback.post_success.streamID:{0}, res_code:{1}, res_text:{2}".format(",".join([item['stream_id'] + '|#|' + item['state'] for item in post_list]), response.status_code, response.text[:100]))
        except Exception as e:
            self.dlog.logger.error('Errro@do_post:{0}'.format(post_list), exc_info=True)

    def deal_state(self, stateinfo):
        try:
            jinfo = json.loads(stateinfo)
            stream_id =jinfo['stream_id']
            code = jinfo['code']
            state = jinfo['state']
            timestamp = jinfo['timestamp']
            if code == 1 and random.random() < 0.99:
                return
            if_post = False
            if stream_id not in self.state_history:
                self.state_history[stream_id] = jinfo
                if_post = True
            else:
                if self.state_history[stream_id]['code'] == code:
                    self.state_history[stream_id]['timestamp'] = timestamp
                else:
                    self.state_history[stream_id] = jinfo
                    if_post = True
            if if_post:
                self.do_post([jinfo])
        except Exception as e:
            self.dlog.logger.error('Error@deal_state:{0}'.format(stateinfo), exc_info=True)

    def start(self):
        self.Runing = True
        while 1:
            if not self.Runing:
                break
            try:
                stateinfo = self.stateQueue.get()
                self.deal_state(stateinfo)
            except Queue.Empty:
                continue
            time.sleep(0.02)

    def stop(self):
        self.Runing = False

