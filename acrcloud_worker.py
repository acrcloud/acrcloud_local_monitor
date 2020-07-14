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
import random
import Queue
import struct
import urllib
import urllib2
import logging
import urlparse
import datetime
import traceback
import threading
import subprocess
from xml.dom import minidom
try:
    from bs4 import BeautifulSoup
except ImportError:
    from BeautifulSoup import BeautifulSoup

from tools_url import Tools_Url
import acrcloud_stream_decode as acrcloud_download
from acrcloud_logger import AcrcloudLogger

reload(sys)
sys.setdefaultencoding("utf8")

class Worker_CollectData(threading.Thread):

    def __init__(self, rec_host, stream_id, stream_url, access_key, access_secret,
                 workQueue, recQueue, shareDict, dlogger, monitor_length=20,
                 monitor_interval=5, monitor_timeout=25, timeout_Threshold = 20):
        threading.Thread.__init__(self)
        self._rec_host = rec_host
        self._stream_id = stream_id
        self._stream_url = stream_url
        self._access_key = access_key
        self._access_secret = access_secret
        self._workQueue = workQueue
        self._recQueue = recQueue
        self._shareDict = shareDict
        self._dlogger = dlogger
        self._monitor_length = monitor_length
        self._monitor_interval = monitor_interval
        self._monitor_timeout = monitor_timeout
        self._timeout_Threshold = timeout_Threshold
        self._timeout_count = 0
        self._running = True
        self.setDaemon(True)

    def getTimestamp(self, monitor_seconds=0):
        nowtime = datetime.datetime.utcnow()
        dsec = datetime.timedelta(seconds=monitor_seconds)
        nowtime = nowtime - dsec
        return nowtime.strftime('%Y-%m-%d %H:%M:%S')

    def run(self):
        self._runing = True
        self._timeout_count = 0
        self._monitor_rec_size = (self._monitor_length + self._monitor_interval)*16000
        self._cur_buffer = ""
        while 1:
            if not self._runing:
                break
            try:
                stream_data = self._workQueue.get()
                self._cur_buffer += stream_data
                if len(self._cur_buffer) >= self._monitor_rec_size:
                    timestamp = self.getTimestamp(self._monitor_length + self._monitor_interval)
                    self._recQueue.put((self._rec_host, self._stream_id, self._stream_url,
                                        self._access_key, self._access_secret,
                                        self._monitor_length, self._cur_buffer, timestamp, self._monitor_interval))
                    self._dlogger.info('MSG@Get_Stream_Buffer(Buffer Size: {0})'.format(len(self._cur_buffer)))
                    self._cur_buffer = ""
            except Exception as e:
                self._dlogger.error('Error@Worker_CollectData.run', exc_info=True)
                continue

    def stop(self):
        self._runing = False

class Worker_DownloadStream(threading.Thread):

    def __init__(self, stream_url, workQueue, cmdQueue, downloadf, dlogger,
                 timeout_sec=10, timeout_Threshold = 15, isFirst=False):
        threading.Thread.__init__(self)
        self._stream_url = stream_url
        self._workQueue = workQueue
        self._cmdQueue = cmdQueue
        self._downloadf = downloadf
        self._dlogger = dlogger
        self._read_size_sec = 5
        self._open_timeout_sec = 30
        self._read_timeout_sec = timeout_sec
        self._timeout_Threshold = timeout_Threshold
        self._invalid_Threshlod = 3
        self._isFirst = isFirst

        self._Runing = True
        self.setDaemon(True)

        self._callback_timeseconds = int(time.time())
        self._callback_interval_threshold = int(self._read_size_sec*0.5)
        self._callback_count = 0
        self._callback_count_threshold = 5
        self._callback_sleep_max_sec = self._read_size_sec
        self._callback_sleep_unit = int(self._callback_sleep_max_sec/self._callback_count_threshold)

    def getPage(self, url, referer=None):
        response = ''
        for i in range(2):
            request = urllib2.Request(url)
            request.add_header("User-Agent", "Mozilla/5.0 (Windows NT 6.1; rv:14.0) Gecko/20100101 Firefox/14.0.1")
            if referer:
                request.add_header("Referer", referer)
            try:
                response = urllib2.urlopen(request)
                if response:
                    result = response.read()
                    response.close()
                    return result
            except Exception, e:
                traceback.print_exc()
                if response:
                    response.close()
        return ''

    def callback(self, isvideo, buf):
        if self._isFirst:
            if isvideo == 1:
                self._cmdQueue.put("ISVIDEO#1#video")
            else:
                self._cmdQueue.put("ISVIDEO#0#audio")
            self._dlogger.warn("Get Stream Type: {0}".format('Video' if isvideo == 1 else 'Audio'))
            self._isFirst = False
        if buf:
            self._workQueue.put(buf)
        if not self._Runing:
            return 1
        return 0

    def callback_new(self, res_dict):
        try:
            ret_code = 0
            if res_dict.get('metadata') != None:
                pass
            else:
                isvideo = res_dict.get('is_video', None)
                buf = res_dict.get('audio_data', None)
                if isvideo is not None and buf is not None:
                    tmp_timeseconds = int(time.time())
                    diff_seconds = tmp_timeseconds - self._callback_timeseconds
                    if diff_seconds < self._callback_interval_threshold:
                        if random.random() < 0.1:
                           self._dlogger.warning('Warn@Worker_DownloadStream.callback_new.call interval too short:{0}'.format(diff_seconds))
                        self._callback_count += 1
                        if self._callback_count > self._callback_count_threshold:
                            time.sleep(self._callback_sleep_max_sec)
                        else:
                            time.sleep(self._callback_count * self._callback_sleep_unit)
                    else:
                        self._callback_count = 0
                    ret_code = self.callback(int(isvideo), buf)
                    self._callback_timeseconds = int(time.time())
                else:
                    self._dlogger.error('Error@Worker_DownloadStream.callback_new.isvideo_or_buf_isNone')
                    time.sleep(5)
        except Exception as e:
            self._dlogger.error('Error@Worker_DownloadStream.callback_new', exc_info=True)
        return ret_code

    def produce_data(self):
        try:
            acrdict = {
                'callback_func':self.callback_new,
                'stream_url':self._stream_url,
                'read_size_sec':self._read_size_sec,
                'open_timeout_sec':self._open_timeout_sec,
                'read_timeout_sec':self._read_timeout_sec,
            }
            code, msg, ffmpeg_code, ffmpeg_msg = self._downloadf.decode_audio(acrdict)
            return code, msg
        except Exception as e:
            self._dlogger.error('Error@Worker_DownloadStream.produce_data', exc_info=True)
            return 10, 'error'

    def run(self):
        self._Runing = True
        self._timeout_count = 0
        self._invalid_count = 0
        while 1:
            if not self._Runing:
                break

            '''
            0, msg = "pause & break"
            1, msg = "avformat_open_input error";
            2, msg = "avformat_find_stream_info error";
            3, msg = "avcodec_find_decoder error";
            4, msg = "avcodec_open2 error";
            5, msg = "av_frame_alloc error";
            6, msg = "swr_init error";
            7, msg = "avcodec_decode_audio4 error";
            8, msg = "av_get_bytes_per_sample error";
            9, msg = "stream url none";
            10, msg = "av_read_frame up timeout count";
            else, msg = "None";
            '''

            self._cmdQueue.put('STATUS#0#running')
            code, msg = self.produce_data()

            if code == 0 or code == '0':
                self._dlogger.error('Error@Worker_DownloadStream.pause get data.{0}'.format(self._stream_url))
                break
            elif code == 1 or code == '1':
                self._invalid_count += 1
                if self._invalid_count > self._invalid_Threshlod:
                    self._dlogger.error('Error@Worker_DownloadStream.invalid_URL.{0}'.format(self._stream_url))
                    self._cmdQueue.put('STATUS#6#bad_url')
                    break
                time.sleep(2)
                continue
            elif code == 10 or code == '10':
                self._timeout_count += 1
                if self._timeout_count > self._timeout_Threshold:
                    self._cmdQueue.put('STATUS#2#dead')
                    break
                self._dlogger.error('Error@Worker_DownloadStream.timeout.{0}'.format(self._stream_url))
                self._cmdQueue.put('STATUS#1#timeout')
                self._invalid_count = 0
                time.sleep(2)
                continue
            elif code == 7 or code == '7':
                self._invalid_count = 0
                self._dlogger.error('Error@Worker_DownloadStream.decode.restart')
                continue
            else:
                self._dlogger.error('Error@Worker_DownloadStream.exit.{0},{1}-{2}'.format(self._stream_url, code, msg))
                self._cmdQueue.put('STATUS#2#dead')
                break
            time.sleep(0.1)

    def stop(self):
        self._Runing = False

class AcrcloudWorker:

    def __init__(self, info, mainqueue, recqueue, shareStatusDict, shareDict, config):
        self._info = info
        self._downloadFun = None
        self._config = config
        self._mainqueue = mainqueue
        self._recqueue = recqueue
        self._shareStatusDict = shareStatusDict
        self._shareDict = shareDict
        self._workQueue = Queue.Queue()
        self._download_cmdQueue = Queue.Queue()
        self._downloadHandler = None
        self._collectHandler = None
        self._stream_id = str(info.get('stream_id', ''))
        self.initLog()
        self.tools_url = Tools_Url()
        self.initConfig(info)
        self.init_url_info()
        self.isFirst = True

    def initLog(self):
        self._dlog = AcrcloudLogger("SWorker_{0}.log".format(self._stream_id), logging.INFO)
        if not self._dlog.addFilehandler(logfile = "SWorker_{0}.log".format(self._stream_id), logdir = self._config['log']['dir']):
            sys.exit(1)
        if not self._dlog.addStreamHandler():
            sys.exit(1)

    def initConfig(self, info):
        try:
            self._dlog.logger.info('initConfig start...')

            self._rec_host = str(info.get('rec_host', ''))
            self._access_key = str(info.get('access_key', ''))
            self._access_secret = str(info.get('access_secret', ''))
            self._stream_url = str(info.get('stream_url', ''))
            self._stream_spare_urls = [url.strip() for url in info.get('stream_spare_urls', []) if url.strip()]
            self._stream_spare_urls += [url.strip() for url in info.get('stream_urls', []) if url.strip()]
            if self._stream_url:
                self._stream_spare_urls = self._stream_spare_urls + [self._stream_url]
            self._stream_spare_urls = list(set(self._stream_spare_urls))

            self._monitor_interval = info.get('interval', 5)
            self._monitor_length = info.get('monitor_length', 20)
            self._monitor_timeout = info.get('monitor_timeout', 30)
            self._timeout_Threshold = 20 #self._config["server"]["timeout_Threshold"]
            self._rec_timeout = info.get('rec_timeout', 5)
            self.baseRebornTime = 20 #self._config["server"]["reborn_Time_Sec"]
            self.rebornTime = 10
            self.deadThreshold = 20 #self._config["server"]["dead_Threshold"]
            self.isFirst = True

            if self._monitor_timeout <= self._monitor_interval + self._monitor_length:
                self._monitor_timeout = self._monitor_interval + self._monitor_length + 2
            self._downloadFun = acrcloud_download
            if not self._downloadFun:
                self._dlog.logger.error('init downloadFunc error')
                self.changeStat(0, "8#error@downloadfun_init")
                sys.exit(1)
        except Exception as e:
            self._dlog.logger.error('Error@AcrcloudWorker.initConfig.failed', exc_info=True)
            sys.exit(1)

    def init_url_info(self):
        try:
            self._url_map = {
                "url_index":-1,
                "url_list":[],
                "url_list_size":0,
                "parse_url_index":-1,
                "parse_url_list":[],
                "parse_url_list_size":0,
                "valid_url_index":set(), #item: (url_index, parse_url_index)
                "valid_url_try":False,
                "rtsp_protocol":["udp", "tcp"],
                "rtsp_protocol_index":0, #一般默认rtsp流是使用udp协议
                "rtsp_protocol_size":2,
            }
            self._stream_url_now = self._stream_url
            if self._stream_url:
                self._url_map["url_list"].append(self._stream_url)
            if self._stream_spare_urls:
                self._url_map["url_list"].extend(self._stream_spare_urls)

            self._url_map["url_index"] = -1
            self._url_map["url_list"] = list(set(self._url_map["url_list"]))
            self._url_map["url_list_size"] = len(self._url_map["url_list"])
            self.change_stream_url()
        except Exception as e:
            self._dlog.logger.error("Error@Worker_DownloadStream.init_url_info", exc_info=True)

    def change_stream_url(self):
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
            self._dlog.logger.warning('Warn@Worker_DownloadStream.change_stream_url.do_change.now_url: {0}\nurl_map: {1}'.format(self._stream_url_now, self._url_map))
        except Exception as e:
            self._dlog.logger.error('Error@Worker_DownloadStream.change_stream_url, url_map: {0}'.format(self._url_map), exc_info=True)

    def changeStat(self, index, msg):
        stat = self._shareStatusDict[self._stream_id]
        stat[index] = msg
        self._shareStatusDict[self._stream_id] = stat

    def newStart(self):
        self._collectHandler = Worker_CollectData(self._rec_host,
                                                  self._stream_id,
                                                  self._stream_url_now,
                                                  self._access_key,
                                                  self._access_secret,
                                                  self._workQueue,
                                                  self._recqueue,
                                                  self._shareDict,
                                                  self._dlog.logger,
                                                  self._monitor_length,
                                                  self._monitor_interval,
                                                  self._monitor_timeout,
                                                  self._timeout_Threshold)
        self._collectHandler.start()

        self._downloadHandler = Worker_DownloadStream(self._stream_url_now,
                                                      self._workQueue,
                                                      self._download_cmdQueue,
                                                      self._downloadFun,
                                                      self._dlog.logger,
                                                      self._monitor_timeout,
                                                      self._timeout_Threshold,
                                                      self.isFirst)
        self.isFirst = False
        self._downloadHandler.start()


    def nowStop(self):
        self._downloadHandler.stop()
        self._collectHandler.stop()

    def deal_mainCMD(self, recv):
        isbreak = False
        if  recv == 'STOP':
            self.nowStop()
            self._dlog.logger.warn("mainQueue receive 'STOP' & JUST STOP")
            isbreak = True
        elif recv == 'PAUSE':
            self.pauseflag = True
            self.nowStop()
            self._dlog.logger.warn("mainQueue receive 'PAUSE' & JUST PAUSE")
            self.changeStat(0, "4#pause")
        elif recv == 'RESTART':
            self.nowStop()
            self.newStart()
            self.pauseflag = False
        return isbreak

    def deal_workerCMD(self, recv_thread):
        isbreak = False
        if recv_thread.startswith("STATUS"):
            status = recv_thread.split("#")
            self.changeStat(0, recv_thread[len('STATUS#'):])
            if status[1] == '2':
                self._dlog.logger.warn("cmdQueue receive 'DEAD' & JUST SLEEP")
                self.nowStop()
                self.deadcount += 1
                self.rebornTime = 2*60 #self.baseRebornTime * self.deadcount
                self.deadflag = True
                self.deadTime = datetime.datetime.now()
                if self.deadcount >= self.deadThreshold:
                    self.killedcount += 1
                    self.killedflag = True
                    self.deadflag = False
                    self.deadcount = 0
                    self.changeStat(0, "3#killed")
                    self._dlog.logger.error("Dead Count Reach Threshold({0}), Monitor will killed".format(self.deadThreshold))
                    self.killedTime = datetime.datetime.now()
            elif status[1] == '3':
                pass
            elif status[1] == '6':
                self._dlog.logger.error("Invalid Stream_Url, This Monitor will wait to retry")
                self.nowStop()
                self.invalid_url_flag = True
                self.invalid_url_time = datetime.datetime.now()
                self.deadflag = False
                self.deadcount = 0
                self.killedflag = False
                self.killedcount = 0
            elif status[1] == '0':
                self.deadcount = 0
                self.killedcount = 0
        elif recv_thread.startswith("ISVIDEO"):
            self.changeStat(1, recv_thread[len('ISVIDEO#'):])
        return isbreak

    def start(self):
        self.newStart()
        self.deadTime = None
        self.deadflag = False
        self.deadcount = 0
        self.pauseflag = False
        self.killedTime = None
        self.killedflag = False
        self.killedcount = 0
        self.killed_reborn_hours = 1
        self.invalid_url_flag = False
        self.invalid_url_time = None
        self.invalid_url_rebornTime = 30 #2 hours
        while 1:
            recv = ''
            recv_thread = ''
            if self.invalid_url_flag:
                invalidpassTime = (datetime.datetime.now() - self.invalid_url_time).total_seconds()
                if invalidpassTime % (20) == 0:
                    self._dlog.logger.warn("Invalid URL Worker Restart Time: {0}s/{1}s".format(invalidpassTime,
                                                                                              self.invalid_url_rebornTime))
                if invalidpassTime >= self.invalid_url_rebornTime:
                    self._dlog.logger.warn("Invalid URL Try Restart...")
                    self.change_stream_url()
                    self.newStart()
                    self.invalid_url_time = None
                    self.invalid_url_flag = False

            if self.deadflag:
                passTime = (datetime.datetime.now() - self.deadTime).seconds
                if passTime % 30 == 0:
                    self._dlog.logger.warn("Worker Reborn Time: {0}s/{1}s".format(passTime, self.rebornTime))
                if passTime >= self.rebornTime:
                    self._dlog.logger.warn("Worker Reborn...")
                    self.change_stream_url()
                    self.newStart()
                    self.deadTime = None
                    self.deadflag = False

            if self.killedflag:
                killedpassTime = (datetime.datetime.now() - self.killedTime).seconds
                if self.killedcount in range(1, 6):
                    self.killed_reborn_hours = 0.03#pow(2, self.killedcount-1)
                elif self.killedcount >= 6:
                    self.killed_reborn_hours = 0.03#pow(2, 5)
                else :
                    self.killed_reborn_hours = 0.03#1
                if killedpassTime % 120 == 0:
                    self._dlog.logger.warn("Killed Worker Reborn Time: {0}/{1} (hours)".format(round(killedpassTime/3600.0, 2), self.killed_reborn_hours))
                if  killedpassTime >= self.killed_reborn_hours*3600:
                    self._dlog.logger.warn("Killed Worker Reborn...")
                    self.change_stream_url()
                    self.newStart()
                    self.killedTime = None
                    self.killedflag = False
            try:
                recv = self._mainqueue.get(block=False)
            except Queue.Empty:
                time.sleep(0.5)
            if self.deal_mainCMD(recv):
                break

            try:
                recv_thread = self._download_cmdQueue.get(block=False)
            except Queue.Empty:
                time.sleep(0.5)
            if self.deal_workerCMD(recv_thread):
                break
            time.sleep(0.1)
