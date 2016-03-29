#!/usr/bin/env python
#-*- coding: utf-8 -*-
#
# author: icy
# date:2015/05/13
#

import sys
import time
import json
import math
import Queue
import random
import struct
import base64
import datetime
import traceback
import threading
import logging
import multiprocessing

from tools_recognize import acrcloud_recognize
from acrcloud_logger import AcrcloudLogger

reload(sys)
sys.setdefaultencoding("utf8")

class Acrcloud_Rec_Worker(threading.Thread):

    def __init__(self, worker_num, shareDict, recognizer, recQueue, resultQueue, dlog):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self._worker_num = worker_num
        self._shareDict = shareDict
        self._recognizer = recognizer
        self._recQueue = recQueue
        self._resultQueue = resultQueue
        self._dlogger = dlog

    def callback_fun(self, result):
        try:
            self._resultQueue.put(result)
        except Exception as e:
            self._dlogger.error('Error@Acrcloud_Rec_Worker.callback_fun', exc_info=True)
        
    def run(self):
        self._running = True
        while 1:
            if not self._running:
                break
            try:
                stream_info = self._recQueue.get() #block = False
            except Queue.Empty:
                continue
            try:
                pem_file_encoded = ""
                if self._shareDict.get("record_"+stream_info[1], [0,0,0])[0]:
                    #pem_file_encoded = base64.b64encode(stream_info[6])
                    pass
                result = {"stream_id": stream_info[1],
                          "stream_url": stream_info[2], 
                          "access_key": stream_info[3], 
                          "result": "",
                          "callback_url": self._shareDict.get("callback_"+stream_info[3], ""),
                          "filter_chinese": int(self._shareDict.get("filter_chinese_"+stream_info[1], 1)),
                          "delay":int(self. _shareDict.get("delay_"+stream_info[1], 1)),
                          "record":self._shareDict.get("record_"+stream_info[1], [0,0,0]),
                          "pem_file":"",
                          "timestamp": stream_info[7]}
                res = self._recognizer.recognize(stream_info[0],
                                                 stream_info[6][:stream_info[5]*16000],
                                                 "fingerprint",
                                                 stream_info[3],
                                                 stream_info[4])
                json_res = json.loads(res)
                if 'response' in json_res and json_res['response']['status']['code'] == 0:
                    result['result'] = json_res
                    self.callback_fun(result)
                    self._dlogger.info('MSG@Worker_Recognize({0}).sendResult.({1}, {2})'.format(self._worker_num,
                                                                                                stream_info[1],
                                                                                                stream_info[7]))
                elif 'status' in json_res and json_res['status']['code'] == 0:
                    result['result'] = json_res
                    self.callback_fun(result)
                    self._dlogger.info('MSG@Worker_Recognize({0}).sendResult.({1}, {2})'.format(self._worker_num,
                                                                                                stream_info[1],
                                                                                                stream_info[7]))
                elif 'status' in json_res and json_res['status']['code'] == 3001:
                    self._dlogger.info('MSG@Worker_Recognize({0}).access_key.error.({1}, {2})'.format(self._worker_num,
                                                                                                      stream_info[1],
                                                                                                      stream_info[7]))
                else:
                    result['result'] = json_res#'noResult'
                    self.callback_fun(result)
                    self._dlogger.info('MSG@Worker_Recognize({0}).noResult.({1}, {2})'.format(self._worker_num,
                                                                                              stream_info[1],
                                                                                              stream_info[7]))
            except Exception as e:
                self._dlogger.error('Error@Worker_Recognize({0})'.format(self._worker_num), exc_info=True)
                self._dlogger.error('Error@result: {0}'.format(json.dumps(result)))
        
    def stop(self):
        self._running = False
        self._dlogger.info('MSG@Acrcloud_Rec_Worker({0}).Delete_Success'.format(self._worker_num))
        
class Acrcloud_Rec_Manager:

    def __init__(self, mainqueue, recqueue, resultqueue, shareDict, config):
        self._mainQueue = mainqueue
        self._recQueue = recqueue
        self._resultQueue = resultqueue
        self._shareDict = shareDict
        self._config = config
        self._recognizer = None
        self._workerpool = []
        self.initLog()
        self.initConfig()
        self.initWorkers(self._init_nums)

    def initLog(self):
        self._dlog = AcrcloudLogger(self._config['log']['recLog'], logging.INFO)
        if not self._dlog.addFilehandler(logfile = self._config['log']['recLog'], logdir = self._config['log']['dir']):
            self.exitRecM('rec_error#0#init_flog_error')
        if not self._dlog.addStreamHandler():
            self.exitRecM('rec_error#0#init_slog_error')
            
    def initConfig(self):
        #self._host = self._config['recognize']['host']
        #self._query_type = self._config['recognize']['query_type']
        cpu_core = multiprocessing.cpu_count()
        self._init_nums = self._config['recognize']['init_nums'].get(str(cpu_core)+'core', 50)
        self._worker_num = 0
        
        #####################
        ## init recognizer ##
        #####################
        self._recognizer = acrcloud_recognize(self._dlog)
        if not self._recognizer:
            self._dlog.logger.error('init recognize error')
            self.exitRecM('rec_error#1#init_recognize_error')
            
    def initWorkers(self, new_nums):
        try:
            for i in range(new_nums):
                rechandler = Acrcloud_Rec_Worker(self._worker_num,
                                                 self._shareDict,
                                                 self._recognizer,
                                                 self._recQueue,
                                                 self._resultQueue,
                                                 self._dlog.logger)
                rechandler.start()
                self._workerpool.append((self._worker_num, rechandler))
                self._worker_num += 1
            self._dlog.logger.warn('Warn@Acrcloud_Rec_Worker(Num:{0}).Init_Success'.format(new_nums))
        except Exception as e:
            self._dlog.logger.error('Error@Init_Rec_Workers', exc_info=True)
            self.exitRecM('rec_error#3#init_rec_workers_error')

    def delWorkers(self):
        try:
            for id, handler in self._workerpool:
                handler.stop()
                self._dlog.logger.warning('Warn@Del_Rec_Worker(ID:{0}/{1})'.format(id, self._worker_num))
                self._worker_num -= 1
        except Exception as e:
            self._dlog.logger.error('Error@Del_Rec_Workers', exc_info=True)
            self._mainQueue.put('rec_error#4#del_rec_workers_error')
            sys.exit(1)

    def exitRecM(self, msg):
        self.delWorkers()
        self._mainQueue.put(msg)
        sys.exit(1)
            
    def start(self):
        while 1:
            try:
                time.sleep(1)
                #deal main_news
            except Queue.Empty:
                time.sleep(2)
            
