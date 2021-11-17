#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author: icy
# Email : qiang@arcloud.com
# Date  : 2016/03/22
#

import os
import re
import sys
import time
import json
import math
import copy
import Queue
import string
import urllib
import urllib2
import requests
import logging
import random
import datetime
import traceback
import threading
import subprocess
import multiprocessing
from random import choice

from tools_url import Tools_Url
from acrcloud_recognizer import Acrcloud_Rec_Worker
#import acrcloud_stream_decode as acrcloud_download
import acrcloud_stream_tool as acrcloud_download
from acrcloud_logger import AcrcloudLogger

reload(sys)
sys.setdefaultencoding("utf8")

class Worker_DownloadStream(threading.Thread):

    def __init__(self, manager_id, stream_info, state_queue, worker_queue, callback_url, config):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self._downloadFun = None
        self._downloadHandler = None
        self._manager_id = manager_id
        self._stream_id = str(stream_info.get('stream_id', ''))
        self._stream_info = stream_info
        self._state_queue = state_queue
        self._worker_queue = worker_queue
        self._callback_url = callback_url
        self._config = config
        self._log_dir = self._config['log']['dir']
        self._stream_rec_type = 0
        self._encode = 0
        if 'recognize' in self._config and 'stream_rec_type' in self._config['recognize'] and self._config['recognize']['stream_rec_type'] in [0, 1]:
            self._stream_rec_type = self._config['recognize']['stream_rec_type']
            if 'encode' in self._config['recognize'] and self._config['recognize']['encode'] in [0, 1]:
                self._encode = self._config['recognize']['encode']

        self.tools_url = Tools_Url()
        self.initLog()

        self.initStreamInfo(self._stream_info)
        self.initConfig()

        self.alive_timestamp = None
        self.alive_is_valid = None
        self.last_status_code = None

        self.set_alive_timestamp()

        self._dlog.logger.warn('Warn@Worker_DownloadStream.init.success.MID:{0}, StreamID:{1}'.format(self._manager_id, self._stream_id))

    def initLog(self):
        self._dlog = AcrcloudLogger('Worker_{0}'.format(self._stream_id)+'(MID:'+str(self._manager_id)+')', logging.INFO)
        if not self._dlog.addFilehandler(logfile = 'Worker_{0}.log'.format(self._stream_id),
                                         logdir = self._log_dir,
                                         loglevel = logging.WARN):
            sys.exit(1)
        if not self._dlog.addStreamHandler():
            sys.exit(1)
        self._dlogger = self._dlog.logger

    def initStreamInfo(self, info):
        self._stream_id = str(info.get('stream_id', ''))
        self._url_map = {
            "url_index":-1,
            "url_list":[],
            "url_list_size":0,
            "parse_url_index":-1,
            "parse_url_list":[],
            "parse_url_list_size":0,
            "valid_url_index":set(),
            "valid_url_try":False,
            "rtsp_protocol":["udp", "tcp"],
            "rtsp_protocol_index":0,
            "rtsp_protocol_size":2,
        }
        stream_url = str(info.get('stream_url', '')).strip()
        self._stream_url_now = stream_url
        stream_spare_urls = [url.strip() for url in info.get('stream_spare_urls', []) if url.strip()]
        if stream_url:
            self._url_map["url_list"].append(stream_url)
        if stream_spare_urls:
            self._url_map["url_list"].extend(stream_spare_urls)

        self._new_refresh = 2

        tmp_url_list = self._url_map["url_list"]
        self._url_map["url_index"] = -1
        self._url_map["url_list"] = list(set(self._url_map["url_list"]))
        self._url_map["url_list"].sort(key = tmp_url_list.index)

        self._url_map["url_list_size"] = len(self._url_map["url_list"])

        self._access_key = str(info.get('access_key', ''))
        self._sinfo = {
            'rec_host': str(info.get('rec_host', '')),
            'access_key': str(info.get('access_key', '')),
            'access_secret': str(info.get('access_secret', '')),
            'stream_id':str(info.get('stream_id')),
            'stream_url':str(info.get('stream_url', '')),
            'stream_rec_type': self._stream_rec_type, #info.get('stream_rec_type', 0),
            'encode': self._encode,
            'stream_spare_urls': self._url_map["url_list"],
            'stream_url_now': self._stream_url_now,
            'monitor_interval': int(info.get('interval', 5)),
            'monitor_length': int(info.get('monitor_length', 20)),
            'monitor_timeout': int(info.get('monitor_timeout', 30)),
            'rec_timeout': int(info.get('rec_timeout', 5)),
            'delay': int(info.get('delay', 1)),
            'filter_lan': 0,
            'record_stream': int(info.get('record_stream', 0)),
            'record': int(info.get('record', 0)),
            'record_before': int(info.get('record_before', 0)),
            'record_after': int(info.get('record_after', 0)),
            'callback_url': info.get('callback_url', ''),
            'post_raw_result': 0,
            'callback_type': int(info.get('callback_type', -1)),
            'rtsp_protocol': info.get('rtsp_protocol', '')
        }
        if self._sinfo['monitor_timeout'] <= self._sinfo['monitor_interval'] + self._sinfo['monitor_length']:
            self._sinfo['monitor_timeout'] = self._sinfo['monitor_interval'] + self._sinfo['monitor_length'] + 5

        self._open_timeout_sec = 60
        self._read_size_sec = self._sinfo['monitor_length'] + self._sinfo['monitor_interval']
        self._read_timeout_sec = self._read_size_sec + 10

        self._callback_time_sec = time.time()
        self._callback_interval_threshold = int(self._read_size_sec*0.2)
        self._callback_count = 0
        self._callback_count_threshold = 5
        self._callback_sleep_max_sec = self._read_size_sec
        self._callback_sleep_unit = int(self._callback_sleep_max_sec/self._callback_count_threshold)

        self.change_stream_url_new()

    def initConfig(self):
        try:
            self._dlog.logger.info('initConfig start...')

            self._break_download = False
            self._restart_download = False
            self._running = True
            self._sleeping = False
            self._isFirst = True

            self._auto_interrupt = True
            self._auto_interrupt_interval = self._config["server"].get("auto_interrupt_interval", 2*60*60)
            self._auto_interrupt_timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            self._auto_interrupt_flag = False
            self._auto_prevent_caching = True
            self._auto_prevent_count = 0
            self._auto_prevent_count_threshold = 30
            self._auto_prevent_time_cost_threshold = 5

            self._invalid_Threshlod = 3
            self._timeout_Threshold = 100
            self._deadThreshold = 100
            self._rebornTime = 0

            self._downloadFun = acrcloud_download
            if not self._downloadFun:
                self._dlog.logger.error('Init downloadFunc Error')
                self.changeStat(0, "8#error@downloadfun_init#0#downloadfun_init")
                sys.exit(1)

        except Exception as e:
            self._dlog.logger.error('Error@Worker_DownloadStream.initConfig.failed', exc_info=True)
            sys.exit(1)

    def initDownloadFun(self):
        try:
            self._downloadFun = acrcloud_download
        except Exception as e:
            self._dlog.logger.error('Error@Worker_DownloadStream.initDownloadFun.failed', exc_info=True)

    def change_stream_url_new(self):
        try:
            if (self._url_map["url_index"], self._url_map["parse_url_index"]) in self._url_map["valid_url_index"] and not self._url_map["valid_url_try"]:
                self._url_map["valid_url_try"] = True
            else:
                if (self._url_map["parse_url_index"] >= 0) and self._stream_url_now.startswith("rtsp") and self._url_map["rtsp_protocol_index"] == 0:
                    self._url_map["rtsp_protocol_index"] += 1
                else:
                    if (self._url_map["parse_url_index"] == -1) or ((self._url_map["parse_url_index"]+1) == self._url_map["parse_url_list_size"]):
                        self._url_map["url_index"] = (self._url_map["url_index"] + 1) % self._url_map["url_list_size"]
                        self._url_map["parse_url_list"] = list(set(self.tools_url.do_analysis_url(self._url_map["url_list"][self._url_map["url_index"]])))
                        self._url_map["parse_url_list_size"] = len(self._url_map["parse_url_list"])
                        self._url_map["parse_url_index"] = 0
                        self._url_map["rtsp_protocol_index"] = 0
                        self._url_map["valid_url_try"] = False
                        self._stream_url_now = self._url_map["parse_url_list"][self._url_map["parse_url_index"]]
                    else:
                        self._url_map["parse_url_index"] += 1
                        self._url_map["rtsp_protocol_index"] = 0
                        self._url_map["valid_url_try"] = False
                        self._stream_url_now = self._url_map["parse_url_list"][self._url_map["parse_url_index"]]
            self._dlog.logger.warning('Warn@Worker_DownloadStream.change_stream_url_new.do_change.now_url: {0}\nurl_map: {1}'.format(self._stream_url_now, self._url_map))
        except Exception as e:
            self._dlog.logger.error('Error@Worker_DownloadStream.change_stream_url_new, url_map: {0}'.format(self._url_map), exc_info=True)

    def changeStat(self, index, msg):
        if self.last_status_code == msg:
            return
        else:
            self.last_status_code = msg

        timestamp = self.get_timestamp()
        self._state_queue.put(('state', (self._access_key, self._stream_id, index, msg, timestamp)))

    def get_timestamp(self, monitor_seconds=0, custom_strftime='%Y-%m-%d %H:%M:%S'):
        nowtime = datetime.datetime.utcnow()
        dsec = datetime.timedelta(seconds=monitor_seconds)
        nowtime = nowtime - dsec
        return nowtime.strftime(custom_strftime)

    def is_auto_interrupt(self):
        if (self._auto_interrupt and random.random() < 0.2):
            time_diff = datetime.datetime.utcnow() - datetime.datetime.strptime(self._auto_interrupt_timestamp, "%Y-%m-%d %H:%M:%S")
            if (time_diff.total_seconds() >= self._auto_interrupt_interval):
                self._auto_interrupt_timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                self._auto_interrupt_flag = True
                self._auto_prevent_caching = True
                self._auto_prevent_count = 0
                self._dlog.logger.warn("@@@@@@@@ auto interrupt @@@@@@@@")
                return True
        return False

    def callback(self, isvideo, buf):
        try:
            self._timeout_count = 0
            self._dead_count = 0
            self._invalid_count = 0
            self._killed_count = 0
            if self._isFirst:
                self.changeStat(-1, "0#running#" + ("1#video" if isvideo == 1 else "0#audio"))
                self._dlog.logger.warn("Warn@Get Stream Type: {0}".format('Video' if isvideo == 1 else 'Audio'))
                self._isFirst = False
            if buf:
                buf_time_sec = len(buf)/16000.0
                timestamp = self.get_timestamp(self._sinfo['monitor_length'] + self._sinfo['monitor_interval'])
                try:
                    self._worker_queue.put((self._sinfo, buf, timestamp, 0, None))
                except Exception as e:
                    self._dlog.logger.error("Error@Worker_DownloadStream.put_queue", exc_info=True)
                    return 1
                if random.random() < 0.3:
                    self._dlog.logger.warn('Warn@Download Stream Buffer(buffer size: {0})'.format(len(buf)))
                elif random.random() < 0.5:
                    self._dlog.logger.info('MSG@Download Stream Buffer(buffer size: {0})'.format(len(buf)))
            if self._break_download or self._restart_download:
                if self._break_download:
                    self._worker_queue.put(({'status':'deleted', 'stream_id':self._sinfo['stream_id']}, '', '', '', None))
                return 1
            if self.is_auto_interrupt():
                return 1
            return 0
        except Exception as e:
            self._dlog.logger.error('Error@Worker_DownloadStream.callback', exc_info=True)

    def callback_new(self, res_dict):
        try:
            self.set_alive_timestamp()

            ret_code = 0
            if res_dict.get('metadata') != None:
                pass
            else:
                isvideo = res_dict.get('is_video', None)
                buf = res_dict.get('audio_data', None)
                if isvideo is not None and buf is not None:
                    tmp_time_sec = time.time()
                    cost_time_sec = tmp_time_sec - self._callback_time_sec
                    if cost_time_sec < self._callback_interval_threshold:
                        if random.random() < 0.1:
                           self._dlog.logger.warning('Warn@Worker_DownloadStream.callback_new.call interval too short:{0}'.format(cost_time_sec))
                        self._callback_count += 1
                        if self._callback_count > self._callback_count_threshold:
                            time.sleep(self._callback_sleep_max_sec)
                        else:
                            time.sleep(self._callback_interval_threshold)
                    else:
                        self._callback_count = 0

                    ret_code = self.callback(int(isvideo), buf)

                    self._url_map["valid_url_index"].add((self._url_map["url_index"], self._url_map["parse_url_index"]))
                else:
                    self._dlog.logger.error('Error@Worker_DownloadStream.callback_new.isvideo_or_buf_isNone')
                    time.sleep(5)
        except Exception as e:
            self._dlog.logger.error('Error@Worker_DownloadStream.callback', exc_info=True)
        return ret_code

    def produce_data(self):
        try:
            self.initDownloadFun()

            self._restart_download = False
            self._callback_count = 0

            self._auto_prevent_caching = True
            self._auto_prevent_count = 0

            if self._break_download:
                time.sleep(0.5)
                return 0, 'stop download', 0, 'ffmpeg exit'

            rtsp_protocol_type = self._sinfo.get('rtsp_protocol', '')
            if not rtsp_protocol_type and self._stream_url_now.startswith("rtsp"):
                rtsp_protocol_type = self._url_map['rtsp_protocol'][self._url_map["rtsp_protocol_index"]]

            acrdict = {
                'callback_func':self.callback_new,
                'stream_url':self._stream_url_now.strip(),
                'read_size_sec':self._read_size_sec,
                'open_timeout_sec':self._open_timeout_sec,
                'read_timeout_sec':self._read_timeout_sec,
                'swr_convert_sec':5,
                'rtsp_transport': rtsp_protocol_type,
                'is_debug':0,
                'extra_opt': {}
            }

            if self._stream_url_now.startswith('rtsp:'):
                acrdict["extra_opt"]['rtsp_transport'] = rtsp_protocol_type

            self._callback_time_sec = time.time()

            code, msg, ffmpeg_code, ffmpeg_msg = self._downloadFun.decode_audio(acrdict)

            return code, msg, ffmpeg_code, ffmpeg_msg
        except Exception as e:
            self._dlog.logger.error('Error@Worker_DownloadStream.produce_data', exc_info=True)
            return 10, 'error', 0, ''

    def deal_download_ret(self, code, msg, ffmpeg_code, ffmpeg_msg):
        retflag = 0
        sleep_flag = False
        sleep_type = ""
        sleep_time = 0
        need_change_url = False
        if code == 0 or code == '0':
            self._dlog.logger.error('Error@Worker_DownloadStream.Stop_getting_data.{0}'.format(self._stream_url_now))
            if self._state_flag == 'stop':
                self.changeStat(0, "10#delete#{0}#{1}".format(ffmpeg_code, ffmpeg_msg))
                retflag = 2
            elif self._state_flag == 'pause':
                self.changeStat(0, "4#pause#{0}#{1}".format(ffmpeg_code, ffmpeg_msg))
                retflag = 2
            if self._auto_interrupt_flag:
                self._auto_interrupt_flag = False
                retflag = 1
        elif code in [1, '1', 11, '11']:
            self._invalid_count += 1
            if self._invalid_count > self._invalid_Threshlod:
                if True:
                    if self._invalid_count >= 4 and self._invalid_count % 2 == 0:
                        self.changeStat(0, "6#invalid_url#{0}#{1}".format(ffmpeg_code, ffmpeg_msg))
                        if self._stream_url_now.find('ttvnw.net') != -1:
                            ip = ""
                        else:
                            ip = self.tools_url.get_ip(self._stream_url_now)
                        headers = self.tools_url.get_header(self._stream_url_now)
                        self._dlog.logger.error('Error@Worker_DownloadStream.Invalid_URL.{0}, invalid_count: {1}, fcode:{2}, fmsg:{3}, ip:{4}, headers:{5}'.format(self._stream_url_now, self._invalid_count, ffmpeg_code, ffmpeg_msg, ip, headers))
                    retflag = 0
                    sleep_flag = True
                    sleep_type = 'invalid_url'
                    sleep_time = 10

                    if self._invalid_count >= 3 and self._invalid_count % 2 == 1:
                        need_change_url = True
            else:
                self._dlog.logger.error('Error@Worker_DownloadStream.Invalid_URL._invalid_count:{0} <= {1}, dont send'.format(self._invalid_count, self._invalid_Threshlod))
                time.sleep(5)
                retflag = 1
        elif code == 10 or code == '10':
            self._invalid_count = 0
            self._timeout_count += 1
            if self._timeout_count > self._timeout_Threshold:
                self._dead_count += 1
                if self._dead_count > self._deadThreshold:
                    self._killed_count += 1

                    self.changeStat(0, "6#invalid_url#{0}#{1}".format(ffmpeg_code, ffmpeg_msg))

                    self._dlog.logger.error('Error@Worker_DownloadStream.Killed.')

                    retflag = 0
                    sleep_flag = True
                    sleep_type = 'killed'
                    sleep_time = 5*60
                    if self._killed_count in range(1, 6):
                        sleep_time = 10*60
                    elif self._killed_count >= 6:
                        sleep_time = 10*60
                    need_change_url = True
                else:
                    self.changeStat(0, "6#invalid_url#{0}#{1}".format(ffmpeg_code, ffmpeg_msg))

                    need_change_url = True
                    self._dlog.logger.error('Error@Worker_DownloadStream.Dead.')
                    retflag = 0
                    sleep_flag = True
                    sleep_type = 'dead'
                    sleep_time = 2*60
            else:
                self.changeStat(0, "1#timeout#{0}#{1}".format(ffmpeg_code, ffmpeg_msg))
                self._dlog.logger.error('Error@Worker_DownloadStream.Timeout.')
                if self._stream_url_now.startswith("rtsp"):
                    need_change_url = True
                if sleep_time > 0:
                    retflag, sleep_flag, sleep_type = 0, True, 'timeout sleep for stream_time_diff'
                else:
                    retflag = 1
                    time.sleep(0.5)
        elif code == 7 or code == '7':
            self._invalid_count = 0
            self._dlog.logger.error('Error@Worker_DownloadStream.Decode_restart.')
            time.sleep(2)
            retflag = 1
        elif code == 8 or code == '8':
            self._dlog.logger.error('Error@get_bytes_per_sample')
            time.sleep(30)
            retflag = 1
        elif code == 12 or code == '12':
            self._dlog.logger.error('Error@Worker_DownloadStream.numsamplesout is zero.')
            time.sleep(2)
            retflag = 1
        elif code == 20 or code == '20':
            self._dlog.logger.error('Error@read stream finished.')
            time.sleep(1)
            retflag = 1
        else:
            self.changeStat(0, "6#invalid_url#{0}#{1}".format(ffmpeg_code, ffmpeg_msg))
            need_change_url = True
            self._dlog.logger.error('Error@Worker_DownloadStream.Exit.{0},{1}-{2}'.format(self._stream_url_now, code, msg))
            retflag = 0
            sleep_flag = True
            sleep_type = 'invalid_url'
            sleep_time = 3*60
        return (retflag, sleep_flag, sleep_type, sleep_time, need_change_url)

    def deal_sleep(self, sleep_type, sleep_time):
        time.sleep(5)
        if sleep_time is None:
            return False
        passTime = (datetime.datetime.now() - self._sleep_start_time).total_seconds()
        if passTime >= sleep_time:
            self._sleep_start_time = None
            self._dlog.logger.warn("Warn@Time_Sleep, Type: {0}, Sleep over, the worker will restart".format(sleep_type))
            return True
        else:
            if passTime % (5*60) == 0:
                self._dlog.logger.info("MSG@Time_Sleep, Type: {0}, PassTime: {1}/{2} s".format(sleep_type, passTime, sleep_time))
            return False

    def run(self):
        self._state_flag = 'run'
        self.init_para(break_flag = False, running_flag = True, sleeping_flag = False)
        while 1:
            if not self._running:
                break
            if self._state_flag == 'run':
                pass
            self._isFirst = True

            self.set_alive_timestamp()

            code, msg, ffmpeg_code, ffmpeg_msg = self.produce_data()

            self.set_alive_timestamp()

            retflag, self._sleeping, sleep_type, sleep_time, need_change_url = self.deal_download_ret(code, msg, ffmpeg_code, ffmpeg_msg)

            if need_change_url:
                self.change_stream_url_new()

            if retflag == 1:
                continue
            elif retflag == 2:
                break

            self._sleep_start_time = datetime.datetime.now()
            while self._sleeping:
                self.set_alive_timestamp()
                break_sleep = self.deal_sleep(sleep_type, sleep_time)
                if break_sleep:
                    break

        self._dlog.logger.warn("Warn@AcrcloudWorker.run.stream_worker_exit")
        self.clean_loghandler()

    def clean_loghandler(self):
        loghandlers = list(self._dlog.logger.handlers)
        for lh in loghandlers:
            self._dlog.logger.removeHandler(lh)
        del self._dlog

    def stop(self):
        self._state_flag = 'stop'
        self.init_para(break_flag = True, running_flag = True, sleeping_flag = False)

    def init_para(self, break_flag, running_flag, sleeping_flag, restart_flag = False):
        self._break_download = break_flag
        self._restart_download = restart_flag
        self._running = running_flag
        self._sleeping = sleeping_flag
        self._timeout_count = 0
        self._dead_count = 0
        self._invalid_count = 0
        self._killed_count = 0

    def get_alive_timestamp(self):
        return self.alive_timestamp

    def set_alive_timestamp(self):
        self.alive_timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

class Worker_SendData(threading.Thread):

        def __init__(self, manager_id, status_queue, state_worker_queue, worker_queue, dlog):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self.manager_id = manager_id
            self.status_queue = status_queue
            self.state_worker_queue = state_worker_queue
            self.worker_queue = worker_queue
            self.dlog = dlog
            self.dlog.logger.warn("Warn@Worker_SendData of manager({0}) init success".format(self.manager_id))

        def run(self):
            self._running = True
            while 1:
                try:
                    try:
                        itype, datainfo = self.state_worker_queue.get(timeout=5)
                    except Queue.Empty:
                        continue

                    if itype == 'state':
                        self.status_queue.put(datainfo, timeout = 2)
                    if random.random() < 0.05:
                        self.dlog.logger.warn("Warn@Worker_SendData.stateQueue size:{0}".format(self.state_worker_queue.qsize()))
                except Exception as e:
                    self.dlog.logger.error("Error@Worker_SendData.put.queue", exc_info=True)

        def stop(self):
            self._running = False

class Worker_Manager:

    def __init__(self, manager_id, result_queue, status_queue, config):
        self._thread_map = {}
        self._manager_id = manager_id
        self._result_queue = result_queue
        self._status_queue = status_queue
        self._worker_state_queue = Queue.Queue()
        self._worker_queue = Queue.Queue()
        self._access_key_map = {}
        self._callback_url_map = {}
        self._config = config

        cpu_core = multiprocessing.cpu_count()
        self._rec_num = 6
        self._log_dir = config['log']['dir']

        self.init_log()

        self._dlog.logger.warn("Warn@Worker_Manager.Init_Success.ID:{0}".format(self._manager_id))

    def init_log(self):
        mlogname = "Worker_Manager_{0}.log".format(self._manager_id)
        self._dlog = AcrcloudLogger(mlogname[:-4], logging.INFO)
        if not self._dlog.addFilehandler(logfile = mlogname[:-4], logdir = self._log_dir, loglevel = logging.WARN):
            sys.exit(1)
        if not self._dlog.addStreamHandler():
            sys.exit(1)

    def init_send_worker(self):
        try:
            sendworker_handler = Worker_SendData(self._manager_id,
                                                 self._status_queue,
                                                 self._worker_state_queue,
                                                 self._worker_queue,
                                                 self._dlog)
            sendworker_handler.start()
        except Exception as e:
            self._dlog.logger.error("Error@Worker_Manager.init_send_worker", exc_info=True)

    def init_recognize_worker(self, rec_num, manager_queue):
        self._rec_index = 0
        self._rec_worker_pool = []
        count = 0
        try:
            tmp_num = self._rec_index + rec_num
            for i in range(rec_num):
                rechandler = Acrcloud_Rec_Worker(self._rec_index, self._worker_queue, self._result_queue, self._log_dir, self._manager_id, manager_queue)
                rechandler.start()
                self._rec_worker_pool.append((self._rec_index, rechandler))
                self._rec_index += 1
                count += 1
            self._dlog.logger.info('MSG@Worker_Manager: init recognize worker success(worker num:{0})'.format(rec_num))
        except Exception as e:
            self._dlog.logger.error('Error@Worker_Manager: init recognize worker error', exc_info=True)
        return count

    def add_worker(self, stream_info):
        try:
            stream_id = stream_info['stream_id']
            access_key = stream_info['access_key']
            if stream_id in self._thread_map:
                self._dlog.logger.warn('Warn@Worker_Manager.add_worker.stream_thread has exists(SID:{0})'.format(stream_id))
                return False

            callback_url = ''
            thread_handler = Worker_DownloadStream(self._manager_id,
                                                   stream_info,
                                                   self._worker_state_queue,
                                                   self._worker_queue,
                                                   callback_url,
                                                   self._config)
            thread_handler.start()
            self._thread_map[stream_id] = thread_handler
            self._access_key_map[stream_id] = access_key
            self._dlog.logger.warn('Warn@Worker_Manager.add_worker.success(stream_id:{0})'.format(stream_id))
            return True
        except Exception as e:
            self._dlog.logger.error('Error@Worker_Manager.addWorker', exc_info=True)
        return False

    def del_worker(self, stream_info):
        try:
            stream_id = stream_info['stream_id']
            if stream_id in self._thread_map:
                thread_handler = self._thread_map[stream_id]
                thread_handler.stop()
                thread_handler.join()
                del self._thread_map[stream_id]
                del self._access_key_map[stream_id]
                self._dlog.logger.warn('Warn@Worker_Manager.del_worker.success(stream_id:{0})'.format(stream_id))
        except Exception as e:
            self._dlog.logger.error('Error@Worker_Manager.delWorker', exc_info=True)

    def add_task(self, itype, info):
        try:
            self._main_queue.put((itype, info))
            return True
        except Exception as e:
            self._dlog.logger.error('Error@Worker_Manager.add_task', exc_info=True)
        return False

    def start(self):
        self._main_queue = multiprocessing.Queue()
        self.smanager_proc = multiprocessing.Process(target=self._do_start, args=(self._status_queue, ))
        self.smanager_proc.start()

    def is_alive(self):
        return self.smanager_proc.is_alive()

    def stop(self):
        self.running = False

    def _do_start(self, status_queue):
        self.running = True
        self._status_queue = status_queue
        self.init_send_worker()
        self.init_recognize_worker(self._rec_num, self._main_queue)
        while self.running:
            try:
                try:
                    itype, stream_info = self._main_queue.get(timeout=5)
                except Queue.Empty:
                    continue
                if itype == 'add':
                    self.add_worker(stream_info)
                elif itype == 'stop':
                    self.del_worker(stream_info)
            except Exception as e:
                self._dlog.logger.error('Error@Worker_Manager._do_start', exc_info=True)
