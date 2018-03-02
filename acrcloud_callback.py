#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# author: johny
# date:2016/01/22
# email: qiang@acrcloud.com

import sys
import ssl
import time
import json
import Queue
import signal
import socket
import random
import requests
import urllib
import urllib2
import datetime
import logging
import traceback
import threading
from random import choice
from acrcloud_config import config as all_config
from acrcloud_logger import AcrcloudLogger as postLogger

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


class PostWorker(threading.Thread):

    def __init__(self, pwid, postQueue, config):
        threading.Thread.__init__(self)
        self.setDaemon(True)

        self.pwid = pwid
        self.config = config
        self.postQueue = postQueue

        self.last_noresult_flag = {}  #保存最近一条结果是否为noresult
        self.timeout = 30

        self.initLog()
        self.dlog.logger.warn("Warn@Init Post Worker Success")

    def initLog(self):
        self.dlog = postLogger("PostLog_{0}".format(self.pwid), logging.INFO)
        if not self.dlog.addFilehandler(logfile = "postLog_{0}.lst".format(self.pwid), logdir=self.config["log"]["dir"]):
            sys.exit(1)
        if not self.dlog.addStreamHandler():
            sys.exit(1)

    def postData_new(self, post_info):
        try:
            jsoninfo, callback_url, itype, send_noresult = post_info
            access_key = jsoninfo.get('access_key')
            stream_id = jsoninfo.get('stream_id')
            stream_url = jsoninfo.get('stream_url')
            timestamp = jsoninfo.get('timestamp')
            result = jsoninfo.get('result')

            isNoResult = True if result['status']['code'] == 1001 else False

            if stream_id not in self.last_noresult_flag:
                self.last_noresult_flag[stream_id] = isNoResult
            else:
                if self.last_noresult_flag[stream_id] == True and isNoResult == True:
                    return
                else:
                    self.last_noresult_flag[stream_id] = isNoResult

            post_dict = {
                "stream_id" : stream_id,
                "stream_url" : stream_url,
                "data" : result,
                "status":0 if isNoResult else 1
            }
            if (int(send_noresult)==1 and isNoResult) or (not isNoResult):
                if int(itype) == 1:
                    #post Form Data
                    post_dict['data'] = json.dumps(post_dict['data'])
                    if callback_url.startswith("https"):
                        response = requests.post(callback_url, data=post_dict, verify=True, timeout=self.timeout)
                    else:
                        response = requests.post(callback_url, data=post_dict, timeout=self.timeout)
                elif int(itype) == 2:
                    #post Json Data
                    headers = {'content-type': 'application/json'}
                    if callback_url.startswith("https"):
                        response = requests.post(callback_url, data=json.dumps(post_dict), verify=True, headers=headers, timeout=self.timeout)
                    else:
                        response = requests.post(callback_url, data=json.dumps(post_dict), headers=headers, timeout=self.timeout)

                parse_title = ""

                self.dlog.logger.info('MSG@postData: [StreamId: {0}, CallBack_URL: {1}, post_type: {2}, isNoResult:{3}, parse_title:{4}], Response code:{5}, text:{6}'.format(stream_id, callback_url, "Form" if int(itype)==1 else "Json", isNoResult, parse_title, response.status_code, response.text[:20]))
        except Exception as e:
            self.dlog.logger.error('Error@postData. streamID:{0}, data:{1}'.format(stream_id, jsoninfo), exc_info=True)

    def parse_acr_result(self, result):
        try:
            title_artist = "None"
            if "status" in result and result["status"]["code"] == 0:
                if "metadata" in result:
                    music = result["metadata"]["music"][0]
                    title = music["title"]
                    artist = music.get("artists", [{"name":""}])[0]["name"]
                    timestamp_utc = result["metadata"]["timestamp_utc"]
                    title_artist = "|#|".join([title, artist, timestamp_utc])
        except Exception as e:
            self.dlog.logger.error('Error@parse_acr_result.error_data: {0}'.format(result), exc_info=True)
        return title_artist

    def run(self):
        while 1:
            try:
                postInfo = self.postQueue.get()
                self.postData_new(postInfo)
                if random.random() < 0.2:
                    postQueueSize = self.postQueue.qsize()
                    self.dlog.logger.info('postQueue Size: {0}'.format(postQueueSize))
            except Queue.Empty:
                pass
            except Exception as e:
                traceback.print_exc()


class PostManager:

    def __init__(self, managerQueue, config=all_config):
        self.managerQueue = managerQueue
        self.config = config
        self.post_worker_num = 8
        self.post_worker_map = {}
        self.assign_index = 0
        self.assign_map = {}
        self.qsize_threshold = 100 #当回调任务队列当中积压超过100个时，发邮件报警
        self.qsize_alert_timestamp = None
        self.qsize_alert_interval = 5*60
        self.initLog()
        self.init_post_worker()
        self.outside_ip = self.get_outside_ip()
        self.dlog.logger.warn("Warn@Init PostManager success!")
        signal.signal(signal.SIGQUIT, self.signal_handler)

    def signal_handler(self, signal, frame):
        self.dlog.logger.error("Receive signal.SIGQUIT, postWorker exit")
        sys.exit(1)

    def initLog(self):
        self.dlog = postLogger("postMananger", logging.INFO)
        if not self.dlog.addFilehandler(logfile = "postManager.lst", logdir = self.config["log"]["dir"]):
            sys.exit(1)
        if not self.dlog.addStreamHandler():
            sys.exit(1)

    def init_post_worker(self):
        try:
            for index in range(self.post_worker_num):
                tmp_post_queue = Queue.Queue()
                tmp_post_handler = PostWorker(index, tmp_post_queue, self.config)
                tmp_post_handler.start()
                self.post_worker_map[index] = (tmp_post_handler, tmp_post_queue)
        except Exception as e:
            self.dlog.logger.error("Error@init_post_worker", exc_info=True)

    def get_outside_ip(self):
        try:
            return urllib2.urlopen('http://ip.42.pl/raw', timeout = 15).read()
        except Exception as e:
            self.dlog.logger.error("Error@get_outside_ip", exc_info=True)
        return "unknow"

    def assign_task(self, data):
        try:
            jsoninfo = json.loads(data)
            access_key = jsoninfo.get('access_key')
            stream_id = jsoninfo.get('stream_id')
            stream_url = jsoninfo.get('stream_url')
            timestamp = jsoninfo.get('timestamp')
            result = jsoninfo.get('result')

            callback_url = jsoninfo.get('callback_url', '')
            callback_type = jsoninfo.get('callback_type', 2) #1.Form, 2.Json
            send_noresult = 0

            isNoResult = True if result['status']['code'] == 1001 else False

            if callback_url.strip() and callback_type is not None:
                if stream_id in self.assign_map:
                    worker_id = self.assign_map[stream_id]
                else:
                    worker_id = self.assign_index % self.post_worker_num
                    self.assign_map[stream_id] = worker_id
                    self.assign_index += 1
                post_worker_queue = self.post_worker_map[worker_id][1]
                post_worker_queue.put((jsoninfo, callback_url.strip(), callback_type, send_noresult))

                #判断回调队列任务数，如果超出则发送邮件
                tmp_queue_size = post_worker_queue.qsize()
                if tmp_queue_size >= self.qsize_threshold:
                    self.do_alert(tmp_queue_size, "Callback Worker ID:{0}, Qsize:{1}".format(worker_id, tmp_queue_size))
        except Exception as e:
            self.dlog.logger.error("Error@assign_task.postInfo:{0}".format(data), exc_info=True)

    def do_alert(self, qsize=0, msg=""):
        try:
            self.dlog.logger.error("Error@do_alert.Callback Task Queue Overstock. size:{0}, msg:{1}".format(qsize, msg))
        except Exception as e:
            self.dlog.logger.error("Error@do_alert", exc_info=True)
        return False

    def start(self):
        while 1:
            try:
                postInfo = self.managerQueue.get()
                self.assign_task(postInfo)
                if random.random() < 0.2:
                    managerQueueSize = self.managerQueue.qsize()
                    self.dlog.logger.info('managerQueue Size: {0}'.format(managerQueueSize))
                    if managerQueueSize >= self.qsize_threshold:
                        self.do_alert(managerQueueSize, "Callback Manageer Qsize:{0}".format(managerQueueSize))
            except Queue.Empty:
                pass
            except Exception as e:
                traceback.print_exc()

def postManager(managerQueue):
    pm = PostManager(managerQueue)
    pm.start()


