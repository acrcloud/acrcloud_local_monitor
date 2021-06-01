#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# author: johny
# date:2016/02/29
# email: qiang@acrcloud.com

import sys
import time
import json
import copy
import Queue
import random
import urllib
import urllib2
import requests
import datetime
import logging
import traceback
import threading
from dateutil.relativedelta import *

from acrcloud_logger import AcrcloudLogger as stateLogger

import socket
socket.setdefaulttimeout(30)

reload(sys)
sys.setdefaultencoding("utf8")

class StateWorker(threading.Thread):

    def __init__(self, config, state_queue, state_callback_url, state_callback_type):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.state_queue = state_queue
        self.state_config = config
        self.state_callback_url = state_callback_url
        self.state_callback_type = state_callback_type
        self.state_log_dir = self.state_config['log']['dir']
        self.state_log = 'state.log'
        self.timeout = 12
        self.initLog()
        self.state_dict = {}
        self.post_state_dict = {}
        self.manager_qsize_dict = {}
        self.dlog.logger.info('INFO@Init_StateWorker Success!')

    def initLog(self):
        self.dlog = stateLogger("StateLog", logging.INFO)
        if not self.dlog.addFilehandler(logfile = self.state_log, logdir = self.state_log_dir):
            sys.exit(1)
        if not self.dlog.addStreamHandler():
            sys.exit(1)

    def check_if_post(self, stream_id):
        try:
            if_post = True
            if stream_id in self.post_state_dict:
                history_post_state = self.post_state_dict[stream_id]
                post_flag = False
                for k in ["state", "type"]:
                    if history_post_state[k] != self.state_dict[stream_id][k]:
                        post_flag = True
                        break
                if post_flag == False and self.state_dict["timestamp"].split(" ")[0] != history_post_state["timestamp"].split(" ")[0]:
                    post_flag = True
                if_post = post_flag
            self.post_state_dict[stream_id] = copy.deepcopy(self.state_dict[stream_id])
        except Exception as e:
            self.dlog.logger.error("Error@check_if_post:{0}".format(stream_id))
        return if_post

    def post_state(self, stream_id, msg, timestamp):
        try:
            if not self.state_callback_url:
                return

            if self.state_dict[stream_id]['state'] in ['running'] and self.state_dict[stream_id]['type'] == 'unknown':
                return

            if self.state_dict[stream_id]['state'] == "timeout" and random.random() < 0.8:
                return

            if not self.check_if_post(stream_id):
                return

            headers = {'content-type': 'application/json'}

            post_list = [self.state_dict[stream_id]]
            post_data = {"status": post_list}
            if self.state_callback_url.startswith("https"):
                response = requests.post(self.state_callback_url, data=json.dumps(post_data), headers=headers, verify=False, timeout=self.timeout)
            else:
                response = requests.post(self.state_callback_url, data=json.dumps(post_data), headers=headers, timeout=self.timeout)

            self.dlog.logger.warn("Warn@state_callback.post_success.streamID:{0}, res_code:{1}, res_text:{2}".format(",".join([item['stream_id']
 + '|#|' + item['state'] for item in post_list]), response.status_code, response.text[:100]))

        except Exception as e:
            self.dlog.logger.error('Error@post_state', exc_info=True)

    def update_state(self, stateinfo):
        try:
            if len(stateinfo) == 2:
                manager_id, qsize = stateinfo
                self.manager_qsize_dict[manager_id] = qsize
            else:
                access_key, stream_id, index, msg, timestamp = stateinfo
                if stream_id not in self.state_dict:
                    self.state_dict[stream_id] = {'access_key' : access_key,
                                                  'stream_id' : stream_id,
                                                  'state' : 'start',
                                                  'code' : 0,
                                                  'type': 'unknown',
                                                  'timestamp' : timestamp,
                                                  'ffmpeg_code' : "",
                                                  'ffmpeg_msg' : ""}
                if index == -1:
                    state_code, state_msg, type_code, type_msg = state_info = msg.split("#")
                    self.state_dict[stream_id]['state'] = state_msg
                    self.state_dict[stream_id]['code'] = int(state_code)
                    self.state_dict[stream_id]['type'] = type_msg
                elif index == 0:
                    state_info = msg.split("#")
                    if len(state_info) == 4:
                        state_code, state_msg, ffmpeg_code, ffmpeg_msg = state_info
                    else:
                        state_code, state_msg = state_info
                        ffmpeg_code, ffmpeg_msg = 0, ""
                    self.state_dict[stream_id]['state'] = state_msg
                    self.state_dict[stream_id]['code'] = int(state_code)
                    self.state_dict[stream_id]['ffmpeg_code'] = str(ffmpeg_code)
                    self.state_dict[stream_id]['ffmpeg_msg'] = ffmpeg_msg
                elif index == 1:
                    type_code, type_msg = msg.split('#')
                    self.state_dict[stream_id]['type'] = type_msg
                self.state_dict[stream_id]['timestamp'] = timestamp
                self.post_state(stream_id, msg, timestamp)
        except Exception as e:
            self.dlog.logger.error('Error@update_state', exc_info=True)

    def run(self):
        while 1:
            try:
                stateinfo = self.state_queue.get()
            except Queue.Empty:
                pass
            self.update_state(stateinfo)

    def get_state(self, stream_id):
        try:
            if stream_id in self.state_dict:
                return self.state_dict[stream_id]
        except Exception as e:
            self.dlog.logger.error('Error@get_state', exc_info=True)
        return None

    def set_pause_state(self, stateinfo):
        try:
            self.state_queue.put(stateinfo)
        except Exception as e:
            self.dlog.logger.error('Error@set_pause_state', exc_info=True)
        return None

    def get_manager_qsize(self):
        try:
            return self.manager_qsize_dict
        except Exception as e:
            self.dlog.logger.error('Error@get_manager_qsize', exc_info=True)
        return None
