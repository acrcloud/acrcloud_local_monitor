#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
  @author: qiang
  @E-mail: qiang@acrcloud.com
  @verion: 8.0.0
  @create: 2016.05.17
"""

import os
import sys
import json
import time
import copy
import Queue
import random
import psutil
import requests
import logging
import datetime
import traceback
import threading
import multiprocessing

from acrcloud_worker import Worker_Manager
from acrcloud_logger import AcrcloudLogger
from acrcloud_state import StateWorker
from acrcloud_result import Acrcloud_Result

reload(sys)
sys.setdefaultencoding("utf8")

from acrcloud_config import config


class AcrcloudMana:
    def __init__ (self, config):
        self.monitor = Acrcloud_Monitor_Main(config)
        self.monitor.start()
        self.sockNum = 0
        self.sockIndex = 1
        self.client2id = {}
        self.id2client = {}
        self.initLog()

    def initLog(self):
        self.colorfmt = "$MAGENTA%(asctime)s$RESET - $RED%(name)-20s$RESET - $COLOR%(levelname)-8s$RESET - $COLOR%(message)s$RESET"
        self.dlog = AcrcloudLogger('Acrcloud@Client', logging.INFO)
        if not self.dlog.addFilehandler(logfile = "ClientRequest.log",
                                        logdir = config["log"]["dir"],
                                        loglevel = logging.WARN):
            sys.exit(1)
        if not self.dlog.addStreamHandler(self.colorfmt):
            sys.exit(1)

    def addClient(self, client):
        #add a client
        self.sockNum = self.sockNum + 1
        self.client2id[client] = self.sockIndex
        self.id2client[self.sockIndex] = client
        self.dlog.logger.info('New Client, ID: {0}'.format(self.sockIndex))
        self.sockIndex = self.sockIndex + 1

    def delClient(self, client):
        #del a client
        if client in self.client2id:
            self.sockNum = self.sockNum - 1
            _sockid = self.client2id[client]
            del self.client2id[client]
            del self.id2client[_sockid]
            self.dlog.logger.info('Close Client, ID: {0}'.format(_sockid))

    def getSockid(self, client):
        #get sockid by client
        if client in self.client2id:
            return self.client2id[client]
        else:
            return None

    def getClient(self, sockid):
        #get client by sockid
        if sockid in self.id2client:
            return self.id2client[sockid]
        else:
            return None

    def recData(self, recdata):
        datainfo = recdata[:-2].split('\r\n', 1)
        if len(datainfo) == 2:
            cmd_info, data_block = datainfo
            cmd_info = cmd_info.split()
            if len(cmd_info) != 5:
                return 'ERROR'
            if cmd_info[0] == 'set':
                ret = ''
                if cmd_info[1] == 'refresh':
                    ret = self.monitor.refresh()
                elif cmd_info[1] == 'stop':
                    ret = self.monitor.stop()
                elif cmd_info[1] == 'ping':
                    ret = self.monitor.ping_state()
                else:
                    ret = "NOT_STORED"
                return ret
            else:
                return "ERROR"
        elif len(datainfo) == 1:
            cmd_info = datainfo[0].split()
            if cmd_info[0] == 'get':
                ret = ''
                if cmd_info[1].startswith('state:'):
                    id = cmd_info[1].split(':')[1]
                    sd = self.monitor.get_status(id.strip())
                    return 'VALUE {0} 0 {1}\r\n{2}'.format(cmd_info[1], len(sd), sd)
                else:
                    return "END"
            else:
                return 'ERROR'
        else:
            return "ERROR"


class Acrcloud_Monitor_Main(threading.Thread):

    def __init__(self, config):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.main_queue = Queue.Queue() #multiprocessing.Queue()
        self.config = config
        self.access_key = config['user']['access_key']
        self.api_url = config['user']['api_url']
        self.stream_ids = config['stream_ids']
        self.managerWorker =  Worker_Manager #managerWorker
        self.result_queue = multiprocessing.Queue()
        self.result_main_queue = multiprocessing.Queue()

        self.callback_url = ''
        self.callback_type = 2
        self.state_callback_url = ''
        self.state_callback_type = 2

        self.refresh_tobj = datetime.datetime.utcnow()
        self.refresh_interval = 2*60*60

        self.init_configuration()
        self.init_log()

        self.init_manager_worker()
        self.init_result()

        time.sleep(2)

        self.init_streams()

        self.init_status_thread()
        self.dlog.logger.info('Init Acrcloud Monitor Main Success')

    def init_log(self):
        colorformat = "$MAGENTA%(asctime)s - $RED%(name)-20s$RESET - $COLOR%(levelname)-8s$RESET - $COLOR%(message)s$RESET"
        self.dlog = AcrcloudLogger('MonitorMain', logging.INFO)
        if not self.dlog.addFilehandler(logfile = "monitor_main.log",
                                        logdir = self.log_dir,
                                        loglevel = logging.INFO):
            sys.exit(1)
        if not self.dlog.addStreamHandler(colorformat):
            sys.exit(1)

    def init_configuration(self):
        cpu_core = multiprocessing.cpu_count()
        self.monitor_manager_num = self.config['server'].get('streamManagerNum', 4)
        self.log_dir = self.config["log"]["dir"]

        self.monitor_index = 0
        self.monitor_dict = dict() # key: stream id, value: stream info
        self.monitor_manager_pool = dict() #key: manager_id, value: manager_proc
        self.status_dict = dict()  # key: stream_id, value: [status_code, status_msg, type_code, type_msg]
        self.callback_url_dict = dict() #key: access_key, value: callback_url

        self.status_queue = multiprocessing.Queue()

    def init_manager_worker(self):
        try:
            for i in range(self.monitor_manager_num):
                smanager_handler = self.managerWorker(i, self.result_queue, self.status_queue, self.config)
                smanager_handler.start()
                if not smanager_handler.is_alive():
                    self.dlog.logger.error('Error@Monitor_main: init manager worker(ID:{0}) failed'.format(i))
                    sys.exit(1)
                self.monitor_manager_pool[i] = smanager_handler
                self.dlog.logger.warn('Warn@Monitor_main: init manager worker(ID:{0}) success'.format(i))
        except Exception as e:
            self.dlog.logger.error('Error@Monitor_main: init manager worker error', exc_info=True)
            sys.exit(1)

    def init_status_thread(self):
        try:
            self.state_handler = StateWorker(self.config, self.status_queue, self.state_callback_url, self.state_callback_type)
            self.state_handler.start()
            self.dlog.logger.warn('Warn@Monitor_main.init state worker success')
        except Exception as e:
            self.dlog.logger.error('Error@Monitor_main.init state worker error', exc_info=True)
            sys.exit(1)

    def init_result(self):
        self.resproc = multiprocessing.Process(target=ResWorker, args=(self.result_main_queue, self.result_queue, self.config))
        self.resproc.start()
        if not self.resproc.is_alive():
            self.dlog.logger.error('Error@AcrcloudMonitor.init_result.failed')
            sys.exit(1)
        else:
            self.dlog.logger.warn('Warn@AcrcloudMonitor.init_result.success')

    def get_timestamp(self, custom_strftime='%Y-%m-%d %H:%M:%S'):
        nowtime = datetime.datetime.now()
        return nowtime.strftime(custom_strftime)

    def get_stream_list_by_api(self, api_url, access_key, stream_ids=[]):
        try:
            url = api_url.format(access_key)
            stream_ids = [ sid.strip() for sid in stream_ids if sid.strip()]
            if len(stream_ids) > 0:
                url += "&stream_ids=" + ",".join(stream_ids)

            r = requests.get(url)
            streaminfo = r.json()
            return json.dumps(streaminfo)
        except Exception as e:
            self.dlog.logger.error('Error@get_stream_list_by_api', exc_info=True)

    def init_streams(self):
        try:
            stream_data = self.get_stream_list_by_api(self.api_url, self.access_key, self.stream_ids)
            info_list = json.loads(stream_data)

            #get access_secret
            access_secret = info_list.get("access_secret")
            if not access_secret:
                self.dlog.logger.error("Error@init_streams.get access_secret failed, exit!")
                sys.exit(1)
            else:
                self.dlog.logger.warn("Warn@init_streams.get access_info success")

            #get callback info
            self.callback_url = info_list.get("callback_url", "")
            self.callback_type = info_list.get("callback_type", 2) #1.Form, 2.Json
            if self.callback_type not in [1, 2, '1', '2']:
                self.callback_type = 2
            #get state callback info
            self.state_callback_url = info_list.get("state_callback_url", "")
            self.state_callback_type = info_list.get("state_callback_type", 2) #1.Form, 2.Json
            if self.state_callback_type not in [1, 2, '1', '2']:
                self.state_callback_type = 2

            new_stream_ids = set()
            old_stream_ids = set(self.monitor_dict.keys())
            #parse jsoninfo to self.shareMonitorDict
            for jsoninfo in info_list.get('streams', []):
                jsoninfo['access_key'] = self.access_key
                jsoninfo['access_secret'] = access_secret
                jsoninfo['callback_url'] = self.callback_url
                jsoninfo['callback_type'] = self.callback_type
                jsoninfo['state_callback_url'] = self.state_callback_url
                jsoninfo['state_callback_type'] = self.state_callback_type
                stream_id = jsoninfo['stream_id']
                new_stream_ids.add(stream_id)
                if stream_id not in self.monitor_dict:
                    if self._do_add_monitor(jsoninfo, False):
                        self.dlog.logger.warn("Add Stream Success:\n {0}".format(jsoninfo))
                    else:
                        self.dlog.logger.error("Add Stream Failed:\n {0}".format(jsoninfo))
        except Exception as e:
            self.dlog.logger.error('Error@init_streams', exc_info=True)

    def refresh(self):
        try:
            for stream_id in self.monitor_dict.keys():
                self._do_del_monitor({'stream_id': stream_id})
            time.sleep(15)
            self.init_streams()
            return 'STORED'
        except Exception as e:
            self.dlog.logger.error('Error@refresh', exc_info=True)
        return 'NOT_STORED'

    def stop(self):
        try:
            for stream_id in self.monitor_dict.keys():
                self._do_del_monitor({'stream_id': stream_id})
            for smanager_handler_key in self.monitor_manager_pool:
                smanager_handler = self.monitor_manager_pool[smanager_handler_key]
                if smanager_handler.is_alive():
                    smanager_handler.stop()
            self.running = False
            return 'STORED'
        except Exception as e:
            self.dlog.logger.error('Error@stop', exc_info=True)
        return 'NOT_STORED'

    def _do_add_monitor(self, jsoninfo, isbackup=True):
        try:
            stream_id = jsoninfo.get('stream_id')
            if stream_id not in self.monitor_dict.keys():
                self.monitor_index += 1
                smanager_handler = self.monitor_manager_pool[self.monitor_index % self.monitor_manager_num]
                smanager_handler.add_task('add', jsoninfo)
                timestamp = self.get_timestamp()
                self.monitor_dict[stream_id] = [self.monitor_index, jsoninfo, timestamp, "add"]
                if isbackup:
                    self.run_backup()
                self.dlog.logger.warn('ADD Stream ({0}, {1})'.format(jsoninfo['stream_id'], jsoninfo['stream_url']))
                return True
        except Exception as e:
            self.dlog.logger.error('Error@Monitor_main._do_add_monitor', exc_info=True)
        self.dlog.logger.error('ADD Stream Failed ({0}, {1})'.format(jsoninfo.get('stream_id'), jsoninfo.get('stream_url')))
        return False

    def _do_del_monitor(self, jsoninfo):
        try:
            stream_id = jsoninfo.get('stream_id')
            stream_index = self.monitor_dict[stream_id][0]
            smanager_handler = self.monitor_manager_pool[stream_index % self.monitor_manager_num]
            smanager_handler.add_task('stop', jsoninfo)
            del self.monitor_dict[stream_id]
            self.dlog.logger.warn('Delete Stream ({0}, {1})'.format(jsoninfo['stream_id'], jsoninfo.get('stream_url')))
            return True
        except Exception as e:
            self.dlog.logger.error('Error@Monitor_main._do_del_monitor', exc_info=True)
        self.dlog.logger.error('Delete Stream Failed ({0}, {1})'.format(jsoninfo.get('stream_id'),jsoninfo.get('stream_url')))
        return False

    def get_status(self, stream_id):
        invalidstat = {'status':1, 'code':5, 'msg':'invalid stream_id',
                       'type':'unknown', 'stream_id':stream_id, 'stream_url':'', 'stream_spare_urls':[],
                       'access_key':'', 'createTime':'', 'callback':'', 'delay':1,
                       'record':0, 'record_before':0, 'record_after':0, 'filter_lan':1}
        try:
            if stream_id == 'all':
                stat = list()
                for id in self.monitor_dict.keys():
                    radiostat = self.get_one_status(id)
                    stat.append(radiostat)
                return json.dumps({"response":stat})

            ids = stream_id.strip().split(',')
            #ids = list(set(ids))
            if len(ids) > 1:
                stat = list()
                for id in ids:
                    if id in self.monitor_dict:
                        radiostat = self.get_one_status(id)
                    else:
                        tmp = invalidstat.copy()
                        tmp['stream_id'] = id
                        radiostat = tmp
                    stat.append(radiostat)
                return json.dumps({"response":stat})
            elif len(ids) == 1:
                if ids[0] in self.monitor_dict:
                    radiostat = self.get_one_status(ids[0])
                    return json.dumps({"response":radiostat})
                else:
                    tmp = invalidstat.copy()
                    tmp['stream_id'] = ids[0]
                    return json.dumps({'response':tmp})

        except Exception as e:
            self.dlog.logger.error('Error@Monitor_main.getStat.failed', exc_info=True)

        return json.dumps({'response':invalidstat})

    def get_one_status(self, stream_id):
        radiostat = {
            "status":0, "code":0, "msg":"running", "type":"unknown",
            "stream_id":stream_id,
            "stream_url":self.monitor_dict[stream_id][1]["stream_url"],
            "stream_spare_urls":self.monitor_dict[stream_id][1].get("stream_spare_urls", []),
            "access_key":self.monitor_dict[stream_id][1]["access_key"],
            "createTime":self.monitor_dict[stream_id][2],
            "record":self.monitor_dict[stream_id][1].get('record', 0),
            "delay":self.monitor_dict[stream_id][1].get('delay', 1),
            "record_before":self.monitor_dict[stream_id][1].get('record_before', 0),
            "record_after":self.monitor_dict[stream_id][1].get('record_after', 0),
            "callback_type":self.monitor_dict[stream_id][1].get('callback_type', -1),
            "post_raw_result":self.monitor_dict[stream_id][1].get('post_raw_result', 0),
            "callback":"",
        }
        state_info = self.state_handler.get_state(stream_id)
        if state_info is not  None:
            radiostat["status"] = state_info['code']
            radiostat["code"] = state_info['code']
            radiostat["msg"] = state_info['state']
            radiostat["type"] = state_info['type']
        if self.monitor_dict[stream_id][1]['access_key'] in self.callback_url_dict:
            radiostat["callback"] = self.callback_url_dict[self.monitor_dict[stream_id][1]['access_key']]
        return radiostat

    def ping_state(self):
        try:
            for smanager_id in self.monitor_manager_pool:
                smanager_handler = self.monitor_manager_pool[smanager_id]
                if not smanager_handler.is_alive():
                    return 'NOT_STORED'
            return 'STORED'
        except Exception as e:
            self.dlog.logger.error("Error@Monitor_main.ping_state", exc_info=True)
        return 'NOT_STORED'

    def watch_smanager(self):
        try:
            for smanager_id in self.monitor_manager_pool:
                smanager_handler = self.monitor_manager_pool[smanager_id]
                if not smanager_handler.is_alive():
                    self.dlog.logger.warn("Warn@Monitor_main.watch_smanager.SID:{0} is not alive. now restart...".format(smanager_id))
                    smanager_handler.start()
                    if smanager_handler.is_alive():
                        for access_key in self.callback_url_dict:
                            smanager_handler.add_task('set_callback_url',
                                                      {'access_key':access_key,
                                                       'callback_url':self.callback_url_dict[access_key]})
                        for stream_id in self.monitor_dict:
                            monitor_index, jsoninfo, timestamp, from_type = self.monitor_dict[stream_id]
                            if from_type == "pause":
                                continue
                            if monitor_index % self.monitor_manager_num == smanager_id:
                                smanager_handler.add_task('add', jsoninfo)
                    time.sleep(0.5)
        except Exception as e:
            self.dlog.logger.error("Error@Monitor_main.watch_smanager", exc_info=True)

    def deal_task(self, itype, jsoninfo_raw):
        try:
            time.sleep(0.1)
        except Exception as e:
            self.dlog.logger.error("Error@Monitor_main.watch", exc_info=True)

    def auto_refresh(self):
        try:
            now_tobj = datetime.datetime.utcnow()
            diff_seconds = (now_tobj - self.refresh_tobj).total_seconds()
            if diff_seconds > self.refresh_interval:
                self.refresh_tobj = now_tobj
                self.refresh()
        except Exception as e:
            self.dlog.logger.error("Error@auto_refresh", exc_info=True)

    def run(self):
        self.running = True
        while self.running:
            try:
                itype, info = self.main_queue.get(timeout=10)
                self.deal_task(itype, info)
            except Queue.Empty:
                pass
            if random.random() < 0.5:
                self.auto_refresh()
            self.watch_smanager()

def ResWorker(mainqueue, resultqueue, config):
    sWorker = Acrcloud_Result(mainqueue, resultqueue, config)
    sWorker.start()

acrcloudMana = AcrcloudMana(config)


