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

    def __init__(self, rec_pool_id, worker_num, shareDict, recognizer, recQueue, resultQueue, dlog):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self._rec_pool_id = rec_pool_id
        self._worker_num = worker_num
        self._shareDict = shareDict
        self._recognizer = recognizer
        self._recQueue = recQueue
        self._resultQueue = resultQueue
        self._dlogger = dlog

    def callback_fun(self, result):
        try:
            self._resultQueue.put(result)
            #print result
        except Exception as e:
            self._dlogger.error('Error@PoolID:{0}.Acrcloud_Rec_Worker.callback_fun'.format(self._rec_pool_id), exc_info=True)

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
                    pem_file_encoded = base64.b64encode(stream_info[6])

                result = {"stream_id": stream_info[1],
                          "stream_url": stream_info[2],
                          "access_key": stream_info[3],
                          "result": "",
                          "callback_url": self._shareDict.get("callback_url_"+stream_info[3], ""),
                          "callback_type": self._shareDict.get("callback_type_"+stream_info[3], 2),
                          "filter_chinese": int(self._shareDict.get("filter_chinese_"+stream_info[1], 1)),
                          "delay":int(self._shareDict.get("delay_"+stream_info[1], 1)),
                          "record":self._shareDict.get("record_"+stream_info[1], [0,0,0]),
                          "monitor_seconds":stream_info[5] + stream_info[8],
                          "pem_file":pem_file_encoded,
                          "timestamp": stream_info[7]}
                #print result
                res = self._recognizer.recognize(stream_info[0],
                                                 stream_info[6][:stream_info[5]*16000],
                                                 "fingerprint",
                                                 stream_info[1],
                                                 stream_info[3],
                                                 stream_info[4])
                try:
                    json_res = json.loads(res)
                except Exception as e:
                    self._dlogger.error("Error@PoolID:{0}.Worker_Recognize({1}).ParseJson.res:{2}".format(
                                                                                                self._rec_pool_id,
                                                                                                self._worker_num,
                                                                                                res))
                    continue

                if 'response' in json_res and json_res['response']['status']['code'] == 0:
                    result['result'] = json_res
                    self.callback_fun(result)
                    self._dlogger.info('MSG@PoolID:{0}.Worker_Recognize({1}).Get_Recognize_Result.({2}, {3})'.format(
                                                                                                self._rec_pool_id,
                                                                                                self._worker_num,
                                                                                                stream_info[1],
                                                                                                stream_info[7]))
                elif 'status' in json_res and json_res['status']['code'] == 0:
                    result['result'] = json_res
                    self.callback_fun(result)
                    self._dlogger.info('MSG@PoolID:{0}.Worker_Recognize({1}).Get_Recognize_Result.({2}, {3})'.format(
                                                                                                          self._rec_pool_id,
                                                                                                          self._worker_num,
                                                                                                          stream_info[1],
                                                                                                          stream_info[7]))
                elif 'status' in json_res and json_res['status']['code'] == 3001:
                    self._dlogger.info('MSG@PoolID:{0}.Worker_Recognize({1}).access_key.error.({2}, {3})'.format(
                                                                                                      self._rec_pool_id,
                                                                                                      self._worker_num,
                                                                                                      stream_info[1],
                                                                                                      stream_info[7]))
                else:
                    result['result'] = json_res#'noResult'
                    self.callback_fun(result)
                    self._dlogger.info('MSG@PoolID:{0}.Worker_Recognize({1}).Get_No_Recognize_Result.({2}, {3})'.format(
                                                                                                        self._rec_pool_id,
                                                                                                        self._worker_num,
                                                                                                        stream_info[1],
                                                                                                        stream_info[7]))
            except Exception as e:
                self._dlogger.error('Error@PoolID:{0}.Worker_Recognize({0})'.format(self._rec_pool_id, self._worker_num), exc_info=True)
                self._dlogger.error('Error@stream_info: {0}'.format(stream_info[:2] if stream_info else "None" ))

    def stop(self):
        self._running = False
        #self._dlogger.info('MSG@Acrcloud_Rec_Worker({0}).Delete_Success'.format(self._worker_num))


class Acrcloud_Rec_Pool:

    def __init__(self, rec_pool_id, poolqueue, resultqueue, shareDict, config):
        self._rec_pool_id = rec_pool_id
        self._poolQueue = poolqueue
        self._resultQueue = resultqueue
        self._shareDict = shareDict
        self._config = config
        self._recognizer = None
        self._workerpool = []
        self._taskQueue = Queue.Queue()  #Manager receive audio and put to taskQueue to rec

        self.initLog()
        self.initConfig()
        self.initWorkers(self._init_nums)
        self._dlog.logger.warn("Rec Pool Init Success, pool_id:{0}".format(self._rec_pool_id))


    def exitRecM(self, msg):
        print msg
        sys.exit(1)

    def initLog(self):
        self._dlog = AcrcloudLogger("RecPool_{0}".format(self._rec_pool_id), logging.INFO)
        if not self._dlog.addFilehandler(logfile = "RecPool_{0}.log".format(self._rec_pool_id), logdir = self._config['log']['dir']):
            self.exitRecM('rec_error#0#init_flog_error, rec_pool_id:{0}'.format(self._rec_pool_id))
        if not self._dlog.addStreamHandler():
            self.exitRecM('rec_error#0#init_slog_error, rec_pool_id:{0}'.format(self._rec_pool_id))

    def initConfig(self):
        #self._host = self._config['recognize']['host']
        #self._query_type = self._config['recognize']['query_type']
        init_nums_map = {'4core':20, '8core':30, '16core':40, '32core':60}
        cpu_core = multiprocessing.cpu_count()
        self._init_nums = init_nums_map.get(str(cpu_core)+'core', 30)
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
                rechandler = Acrcloud_Rec_Worker(self._rec_pool_id,
                                                 self._worker_num,
                                                 self._shareDict,
                                                 self._recognizer,
                                                 self._taskQueue,
                                                 self._resultQueue,
                                                 self._dlog.logger)
                rechandler.start()
                self._workerpool.append((self._worker_num, rechandler))
                self._worker_num += 1
            self._dlog.logger.warn('Warn@PoolID:{0}.initWorkers.Init_Success.(Num:{1})'.format(self._rec_pool_id, new_nums))
        except Exception as e:
            self._dlog.logger.error('Error@initWorkers', exc_info=True)
            self.exitRecM('rec_error#3#init_rec_pool_workers_error')

    def delWorkers(self):
        try:
            count = 0
            for id, handler in self._workerpool:
                handler.stop()
                self._worker_num -= 1
                count += 1
            self._dlog.logger.warning('Warn@Del_Rec_Pool_Workers_Success.(Totle Num:{0})'.format(count))
        except Exception as e:
            self._dlog.logger.error('Error@Del_Rec_Pool_Workers', exc_info=True)

    def addTask(self, recinfo):
        try:
            self._taskQueue.put(recinfo)
            if random.random() < 0.1:
                self._dlog.logger.warn("Warn@Pool.addTask.PoolQSize:{0}, TaskQSize:{1}".format(self._poolQueue.qsize(), self._taskQueue.qsize()))
        except Exception as e:
            self._dlog.logger.error('Error@Pool.addTask', exc_info=True)

    def start(self):
        self._running = True
        while 1:
            if not self._running:
                break
            try:
                itype, recinfo = self._poolQueue.get()
                if itype == "cmd" and recinfo == 'stop':
                    self.stop()
                else:
                    self.addTask(recinfo)
            except Queue.Empty:
                pass

    def stop(self):
        self.delWorkers()
        self._running = False
        self._dlog.logger.warn('Warn@Acrcloud_Recoginze_Pool_Stop')
        sys.exit(1)


def poolWorker(rec_pool_id, poolqueue, resultqueue, shareDict, config):
    pWorker = Acrcloud_Rec_Pool(rec_pool_id, poolqueue, resultqueue, shareDict, config)
    pWorker.start()

class Acrcloud_Rec_Manager:

    def __init__(self, mainqueue, recqueue, resultqueue, shareDict, config):
        self._mainQueue = mainqueue
        self._recQueue = recqueue
        self._resultQueue = resultqueue
        self._shareDict = shareDict
        self._config = config
        self._rec_pool = []
        self.initLog()

        self.stream_assign_index = 0
        self.stream_assign_map = {}

        self._init_pool_num = 3
        self.initPoolWorkers()

    def exitRecM(self, msg):
        print msg
        sys.exit(1)

    def initLog(self):
        self._dlog = AcrcloudLogger("RecManager", logging.INFO)
        if not self._dlog.addFilehandler(logfile = "RecManager.log", logdir = self._config['log']['dir']):
            self.exitRecM('rec_error#0#init_flog_error')
        if not self._dlog.addStreamHandler():
            self.exitRecM('rec_error#0#init_slog_error')

    def initPoolWorkers(self):
        try:
            for i in range(self._init_pool_num):
                tmp_poolQueue = multiprocessing.Queue()
                pool_proc = multiprocessing.Process(target=poolWorker,
                                                    args=(i,
                                                          tmp_poolQueue,
                                                          self._resultQueue,
                                                          self._shareDict,
                                                          self._config))
                pool_proc.daemon = True
                pool_proc.start()
                if not pool_proc.is_alive():
                    self._dlog.logger.error('Error@initPoolWorkers.init_rec_pool:{0}.failed'.format(i))
                    sys.exit(1)
                else:
                    self._dlog.logger.warn('Warn@initPoolWorkers.init_rec_pool:{0}.success'.format(i))

                self._rec_pool.append((i, tmp_poolQueue, pool_proc))
            self._dlog.logger.warn('Warn@initPoolWorkers.Init_Success.(Total Num:{0})'.format(self._init_pool_num))
        except Exception as e:
            self._dlog.logger.error('Error@initPoolWorkers', exc_info=True)
            self.exitRecM('rec_error#3#init_rec_workers_error')

    def delPoolWorkers(self):
        try:
            for id, pool_queue, pool_proc in self._rec_pool:
                pool_queue.put(('cmd', 'stop'))
                self._dlog.logger.warning('Warn@Del_Rec_PoolWorkers.send_stop_cmd_to_pool.(pool_id:{0})'.format(id))
        except Exception as e:
            self._dlog.logger.error('Error@Del_Rec_PoolWorkers', exc_info=True)

    def addTask(self, recinfo):
        try:
            stream_id = recinfo[1]
            if stream_id not in self.stream_assign_map:
                tmp_index = self.stream_assign_index % self._init_pool_num
                self.stream_assign_map[stream_id] = tmp_index
                self.stream_assign_index += 1

            pool_index = self.stream_assign_map[stream_id]
            pool_queue = self._rec_pool[pool_index][1]
            pool_queue.put(('rec', recinfo))
            if random.random() < 0.1:
                self._dlog.logger.warn("Warn@addTask.RecQSize:{0}, PoolID:{1}, PoolQSize:{2}".format(self._recQueue.qsize(), pool_index, pool_queue.qsize()))
        except Exception as e:
            self._dlog.logger.error('Error@addTask', exc_info=True)

    def start(self):
        self._running = True
        while 1:
            if not self._running:
                break
            try:
                cmdinfo = self._mainQueue.get(block=False)
                if cmdinfo[0] == 'stop':
                    self.stop()
            except Queue.Empty:
                time.sleep(0.01)
            try:
                recinfo = self._recQueue.get(block=False)
                self.addTask(recinfo)
            except Queue.Empty:
                time.sleep(0.01)


    def stop(self):
        self.delPoolWorkers()
        self._running = False
        self._dlog.logger.warn('Warn@Acrcloud_Recoginze_Manager_Stop')
        sys.exit(1)
