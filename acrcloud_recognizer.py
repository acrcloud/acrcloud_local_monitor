#!/usr/bin/env python
#-*- coding: utf-8 -*-
#
# author: icy
# date:2016/03/22
#

import sys
import time
import json
import math
import Queue
import base64
import random
import struct
import urlparse
import datetime
import traceback
import threading
import logging
import multiprocessing

from tools_recognizer import acrcloud_recognize
from acrcloud_logger import AcrcloudLogger

reload(sys)
sys.setdefaultencoding("utf8")

class Acrcloud_Rec_Worker(threading.Thread):
    def __init__(self, worker_num, recQueue, resultQueue, log_dir, smanager_id, smanager_queue):
        threading.Thread.__init__(self)
        self.setDaemon(True)

        self.worker_num = worker_num
        self.recQueue = recQueue
        self.resultQueue = resultQueue
        self.log_dir = log_dir
        self.smanager_id = smanager_id
        self.smanager_queue = smanager_queue
        self.initLog()
        self.initConfig()

    def initLog(self):
        self.dlog = AcrcloudLogger("RecWorker(ID:{0})".format(str(self.smanager_id)+"_"+str(self.worker_num)), logging.INFO)
        if not self.dlog.addFilehandler(logfile = "RecWorker_{0}.log".format(str(self.smanager_id)+"_"+str(self.worker_num)),
                                        logdir = self.log_dir, interval=5):
            sys.exit(1)
        if not self.dlog.addStreamHandler():
            sys.exit(1)

    def initConfig(self):
        self.recognizer = acrcloud_recognize(self.dlog)
        if not self.recognizer:
            self.dlog.logger.error('init recognize error')
            sys.exit(1)

    def callback_fun(self, result):
        try:
            self.resultQueue.put(result)
        except Exception as e:
            self.dlog.logger.error('Error@callback_fun', exc_info=True)

    def do_spl_rec(self, rec_host, audio_buf, stream_id, access_key, access_secret, encode):
        try:
            need_update, json_new_res, new_res = False, None, None
            #用音频进行识别
            new_res = self.recognizer.recognize_new(rec_host, audio_buf, 'audio', stream_id, access_key, access_secret, encode)
            if new_res:
                json_new_res = json.loads(new_res)
                if 'status' in json_new_res and json_new_res['status']['code'] == 0:
                    need_update = True
                    self.dlog.logger.warning("do_spl_rec.get_result:{0}, res: Yes".format(stream_id))
        except Exception as e:
            self.dlog.logger.error('Error@do_spl_rec:{0}, {1}'.format(stream_id, access_key), exc_info=True)
        return need_update, json_new_res

    def do_rec(self, datainfo):
        try:
            stream_info, buffer, timestamp, pitch_shift_flag, stream_metadata = datainfo[:5]

            if len(buffer) <= 10:
                self.dlog.logger.warn('Warn@Worker_Recognize({0}).Get_None_Stream_Buffer.({1})'.format(self.worker_num, stream_info['stream_id']))
                return
            buffer_duration = round(len(buffer)*1.0/16000, 2)
            pem_file_encoded = ""
            if stream_info['record'] >=1 or int(stream_info.get("record_stream", 0)) == 1:
                pem_file_encoded = base64.b64encode(buffer)

            stream_rec_type = stream_info.get("stream_rec_type", 0)
            encode = stream_info.get("encode", 0)

            result = {
                "stream_id": stream_info['stream_id'],
                "stream_url": stream_info['stream_url'],
                "access_key": stream_info['access_key'],
                "monitor_seconds": stream_info['monitor_length'] + stream_info['monitor_interval'],
                "result": '',
                "filter_chinese": int(stream_info['filter_lan']),
                "delay": stream_info['delay'],
                "record": [stream_info['record'], stream_info['record_before'], stream_info['record_after']],
                "callback_url": stream_info['callback_url'],
                "callback_type": int(stream_info.get("callback_type", -1)),
                "pem_file": pem_file_encoded,
                "timestamp": timestamp,
            }

            retfp = True

            res, fp_buf = self.recognizer.recognize(stream_info['rec_host'],
                                                    buffer[:stream_info['monitor_length']*16000],
                                                    'fingerprint',
                                                    stream_info['stream_id'],
                                                    stream_info['access_key'],
                                                    stream_info['access_secret'])

            if res is None or res == '':
                result['result'] = {"status": {"msg": "No result", "version": "1.0", "code": 1001}}
                self.callback_fun(result)
                self.dlog.logger.warn('Warn@Worker_Recognize({0}).Get_None_Rec_Result.({1})'.format(self.worker_num, stream_info['stream_id']))
                return

            try:
                json_res = json.loads(res)
            except Exception as e:
                self.dlog.logger.error('Error@Worker_Recognize({0}).parse_rec_json.({1}), error rec:{2}'.format(self.worker_num, stream_info['stream_id'], res))
                return

            try:
                if 'status' in json_res and json_res['status'].get('code') == 1001:
                    if stream_rec_type == 1:
                        need_update, new_json_res = self.do_spl_rec(stream_info['rec_host'],
                                                                    buffer,
                                                                    stream_info['stream_id'],
                                                                    stream_info['access_key'],
                                                                    stream_info['access_secret'],
                                                                    encode)
                        if need_update:
                            json_res = new_json_res
            except Exception as e:
                self.dlog.logger.error("Error@do_rec.do_spl_rec:{0}, {1}, {2}".format(stream_id, access_key, api_type, raw_access_key), exc_info=True)


            if 'response' in json_res and json_res['response']['status']['code'] == 0:
                result['result'] = json_res
                self.callback_fun(result)
                self.dlog.logger.info('MSG@Worker_Recognize({0}).sendResult.({1}, {2})'.format(self.worker_num,
                                                                                               stream_info['stream_id'],
                                                                                               timestamp))
            elif 'status' in json_res and json_res['status']['code'] == 0:
                result['result'] = json_res
                self.callback_fun(result)
                self.dlog.logger.info('MSG@Worker_Recognize({0}).sendResult.({1}, {2})'.format(self.worker_num, stream_info['stream_id'], timestamp))
            elif 'status' in json_res and json_res['status']['code'] == 3001:
                self.dlog.logger.info('MSG@Worker_Recognize({0}).access_key.error.({1}, {2})'.format(self.worker_num, stream_info['stream_id'], timestamp))
            else:
                result['result'] = json_res#'noResult'
                self.callback_fun(result)
                self.dlog.logger.info('MSG@Worker_Recognize({0}).noResult.({1}, {2}, {3}, {4})'.format(self.worker_num, stream_info['stream_id'], timestamp, json_res['status']['code'], json_res['status']['msg']))
        except Exception as e:
            self.dlog.logger.error('Error@Worker_Recognize({0})'.format(self.worker_num), exc_info=True)


    def run(self):
        self.running = True
        while 1:
            if not self.running:
                break
            try:
                datainfo = self.recQueue.get() #block = False
                if random.random() < 0.3:
                    recqueue_size = self.recQueue.qsize()
                    if random.random() < 0.5:
                        self.dlog.logger.info('RecQueue Size: {0}'.format(recqueue_size))
            except Queue.Empty:
                continue
            self.do_rec(datainfo)

    def stop(self):
        self.running = False
        self.dlog.logger.info('MSG@Acrcloud_Rec_Worker({0}).Delete_Success'.format(self.worker_num))
        self.clean_loghandler()

    def clean_loghandler(self):
        loghandlers = list(self.dlog.logger.handlers)
        for lh in loghandlers:
            self.dlog.logger.removeHandler(lh)
        del self.dlog
