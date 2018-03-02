#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
  @author johnny
  @E-mail qiang@acrcloud.com
  @version v4-local
  @create 2016.03.08
"""

import os
import sys
import json
import time
import Queue
import urllib
import urllib2
import logging
import traceback
import multiprocessing

from acrcloud_worker import AcrcloudWorker
from acrcloud_recognize import Acrcloud_Rec_Manager
from acrcloud_logger import AcrcloudLogger
from acrcloud_result import Acrcloud_Result
from acrcloud_config import config

reload(sys)
sys.setdefaultencoding("utf8")

class AcrcloudManager:
    def __init__ (self, springboard):
        self.monitor = springboard
        self.sockNum = 0
        self.sockIndex = 1
        self.client2id = {}
        self.id2client = {}
        self.initLog()

    def initLog(self):
        self.colorfmt = "$MAGENTA%(asctime)s$RESET - $RED%(name)-20s$RESET - $COLOR%(levelname)-8s$RESET - $COLOR%(message)s$RESET"
        self.dlog = AcrcloudLogger('Client@Main', logging.INFO)
        if not self.dlog.addStreamHandler(self.colorfmt):
            sys.exit(1)

    def addClient(self, client):
        self.sockNum = self.sockNum + 1
        self.client2id[client] = self.sockIndex
        self.id2client[self.sockIndex] = client
        self.dlog.logger.info('New Client, ID: {0}'.format(self.sockIndex))
        self.sockIndex = self.sockIndex + 1

    def delClient(self, client):
        if client in self.client2id:
            self.sockNum = self.sockNum - 1
            _sockid = self.client2id[client]
            del self.client2id[client]
            del self.id2client[_sockid]
            self.dlog.logger.info('Close Client, ID: {0}'.format(_sockid))

    def getSockid(self, client):
        if client in self.client2id:
            return self.client2id[client]
        else:
            return None

    def getClient(self, sockid):
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
                if cmd_info[1] == 'restart':
                    ret = self.monitor.reStart(data_block)
                elif cmd_info[1] == 'refresh':
                    ret = self.monitor.reFresh()
                elif cmd_info[1] == 'pause':
                    ret = self.monitor.pauseM(data_block)
                elif cmd_info[1] == 'stop':
                    ret = self.monitor.stop()
                else:
                    ret = "NOT_STORED"
                return ret
            else:
                return "ERROR"
        elif len(datainfo) == 1:
            cmd_info = datainfo[0].split()
            if cmd_info[0] == 'get':
                ret = ''
                if cmd_info[1].startswith('state-'):
                    id = cmd_info[1].split('-')[1]
                    sd = self.monitor.getStat(id.strip())
                    return 'VALUE {0} 0 {1}\r\n{2}'.format(cmd_info[1], len(sd), sd)
                else:
                    return "END"
            else:
                return 'ERROR'
        else:
            return "ERROR"


class AcrcloudSpringboard:

    def __init__(self, manager, config, dworker, rworker, sworker):
        self.manager = manager
        self.config = config
        self.dworker = dworker
        self.rworker = rworker
        self.sworker = sworker
        self.access_key = self.config['user']['access_key']
        #self.access_secret = self.config['user']['access_secret']
        self.api_url = self.config['user']['api_url']
        self.record = int(self.config['record']['record'])
        self.record_before = int(self.config['record']['record_before'])
        self.record_after = int(self.config['record']['record_after'])
        self.addkeys =['access_key','access_secret','rec_host','stream_id','stream_url',
                       'interval','monitor_length','monitor_timeout','rec_timeout']
        self.mainQueue = multiprocessing.Queue()
        self.shareStatusDict = multiprocessing.Manager().dict()
        self.shareMonitorDict = multiprocessing.Manager().dict()
        self.shareDict = multiprocessing.Manager().dict()
        self.initLog()
        self.initManager()
        self.initStreams()

    def initLog(self):
        self.colorfmt = "$MAGENTA%(asctime)s - $RED%(name)-20s$RESET - $COLOR%(levelname)-8s$RESET - $COLOR%(message)s$RESET"
        self.dlog = AcrcloudLogger('Acrcloud@Springboard', logging.INFO)
        if not self.dlog.addStreamHandler(self.colorfmt):
            sys.exit(1)

    def initManager(self):
        try:
            self.manager_proc = multiprocessing.Process(target = self.manager,
                                                        args = (self.mainQueue,
                                                                self.config,
                                                                self.shareMonitorDict,
                                                                self.shareStatusDict,
                                                                self.shareDict,
                                                                self.dworker,
                                                                self.rworker,
                                                                self.sworker))
            self.manager_proc.start()
            if not self.manager_proc.is_alive():
                self.dlog.logger.error('Error@Springboard:create manager process failed, it will stop')
                sys.exit(1)
            else:
                self.dlog.logger.warn('Warn@Springboard:manager init success')
        except Exception as e:
            self.dlog.logger.error('Error@Springboard:init manager failed, it will stop', exc_info=True)
            sys.exit(1)

    def checkInfo(self, info):
        if len(info) >= 8:
            for key in self.addkeys:
                if info.get(key, 'None') == 'None':
                    return False
            return True
        return False

    def changeStat(self, id, index, msg):
        stat = self.shareStatusDict[id]
        stat[index] = msg
        self.shareStatusDict[id] = stat

    def changeMon(self, id, index, value):
        tmp = self.shareMonitorDict[id]
        tmp[index] = value
        self.shareMonitorDict[id] = tmp

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

    def getStreamInfo(self, api_url, access_key):
        try:
            url = api_url.format(access_key)
            datainfo = self.getPage(url)
            streaminfo = json.loads(datainfo)
            return json.dumps(streaminfo)
        except Exception as e:
            self.dlog.logger.error('Error@Springboard.getStreamInfo', exc_info=True)

    def initStreams(self):
        try:
            stream_data = self.getStreamInfo(self.api_url, self.access_key)
            info_list = json.loads(stream_data)

            #get access_secret
            access_secret = info_list.get("access_secret")
            if not access_secret:
                self.dlog.logger.error("Error@Springboard.initStreams.get access_secret failed, exit!")
                sys.exit(1)
            else:
                self.dlog.logger.warn("Warn@Springboard.initStraems.get access_info success")

            #get callback info
            callback_url = info_list.get("callback_url", "")
            callback_type = info_list.get("callback_type", 2) #1.Form, 2.Json
            self.shareDict["callback_url_"+self.access_key] = callback_url
            self.shareDict["callback_type_"+self.access_key] = callback_type
            self.dlog.logger.warn("Warn@Springboard.initStreams.callback_info.(callback_url:{0}, callback_type:{1})".format(callback_url, callback_type))

            #parse jsoninfo to self.shareMonitorDict
            for jsoninfo in info_list.get('streams', []):
                jsoninfo['access_key'] = self.access_key
                jsoninfo['access_secret'] = access_secret
                stream_id = jsoninfo['stream_id']
                createTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                self.shareMonitorDict[stream_id] = [jsoninfo, createTime, 0]
                self.shareStatusDict[stream_id] = ['0#running', '2#unknow']
                self.shareDict['filter_chinese_'+stream_id] = jsoninfo.get('filter_chinese', 0)
                self.shareDict['delay_'+stream_id] = int(jsoninfo.get('delay', 1))
                self.shareDict['record_'+stream_id] = [self.record, self.record_before, self.record_after]
                self.dlog.logger.info('MSG@Springboard.initStreams.add one stream({0}) success'.format(stream_id))
            self.mainQueue.put(('heiheihei', ''))
        except Exception as e:
            self.dlog.logger.error('Error@Springboard.initStreams', exc_info=True)

    def reFresh(self):
        try:
            self.dlog.logger.warn('Warn@Springboard.reFresh start...')
            stream_data = self.getStreamInfo(self.api_url, self.access_key)
            info_list = json.loads(stream_data)

            #get access_secret
            access_secret = info_list.get("access_secret")
            if not access_secret:
                self.dlog.logger.error("Error@Springboard.reFresh.get access_secret failed, exit!")
                sys.exit(1)
            else:
                self.dlog.logger.warn("Warn@Springboard.reFresh.get access_info success")

            #get callback info
            callback_url = info_list.get("callback_url", "")
            callback_type = info_list.get("callback_type", 2) #1.Form, 2.Json
            self.shareDict["callback_url_"+self.access_key] = callback_url
            self.shareDict["callback_type_"+self.access_key] = callback_type
            self.dlog.logger.warn("Warn@Springboard.reFresh.callback_info.(callback_url:{0}, callback_type:{1})".format(callback_url, callback_type))

            new_stream_ids = set()
            for jsoninfo in info_list.get('streams', []):
                jsoninfo['access_key'] = self.access_key
                jsoninfo['access_secret'] = access_secret
                stream_id = jsoninfo.get('stream_id')
                if not stream_id:
                    continue
                self.shareStatusDict[stream_id] = ['0#refresh', '2#unknow']
                self.shareDict['filter_chinese_'+stream_id] = jsoninfo.get('filter_chinese', 1)
                self.shareDict['delay_'+stream_id] = int(jsoninfo.get('delay', 1))
                createTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                new_stream_ids.add(stream_id)
                if stream_id in self.shareMonitorDict:
                    pre_jsoninfo = self.shareMonitorDict[stream_id][0]
                    pre_monitor_str = self.gen_monitor_str(pre_jsoninfo)
                    curr_monitor_str = self.gen_monitor_str(jsoninfo)
                    if curr_monitor_str == pre_monitor_str:
                        self.changeMon(stream_id, 2, 0) #0代表没变
                    else:
                        self.shareMonitorDict[stream_id] = [jsoninfo, createTime, 1] #1代表有修改
                else:
                    self.shareMonitorDict[stream_id] = [jsoninfo, createTime, 2] #2代表添加
            monitor_stream_ids = set(self.shareMonitorDict.keys())
            del_stream_ids = monitor_stream_ids - new_stream_ids
            self.dlog.logger.info('Del_stream_ids:{0}'.format(','.join(del_stream_ids)))
            for del_stream_id in del_stream_ids:
                self.changeMon(del_stream_id, 2, 3) #3代表删除
            self.mainQueue.put(('refresh', ''))
            return 'STORED'
        except Exception as e:
            self.dlog.logger.error('Error@Springboard.refresh_streams', exc_info=True)
        return 'NOT_STORED'

    def gen_monitor_str(self, jsoninfo):
        key_list = ['monitor_length', 'interval', 'monitor_timeout', 'rec_timeout']
        monitor_str_list = [jsoninfo.get('rec_host', ''), jsoninfo.get('stream_url', '')]
        for key in key_list:
            value = jsoninfo.get(key, 0)
            if value is True:
                value = 1
            if value is False:
                value = 0
            monitor_str_list.append(str(value))
        return '|#|'.join(monitor_str_list)

    def reStart(self, info):
        try:
            jsoninfo = json.loads(info)
            stream_id = jsoninfo.get('stream_id')
            if stream_id in self.shareMonitorDict:
                code, msg = self.shareStatusDict[jsoninfo['stream_id']][0].split('#')
                if code == '4' or code == '3' or code == '6':
                    self.mainQueue.put(('restart', jsoninfo))
                    return 'STORED'
        except Exception as e:
            self.dlog.logger.error('Error@Springboard.restart_stream', exc_info=True)
        return 'NOT_STORED'

    def pauseM(self, info):
        try:
            jsoninfo = json.loads(info)
            stream_id = jsoninfo.get('stream_id')
            if stream_id in self.shareMonitorDict:
                code, msg = self.shareStatusDict[stream_id][0].split('#')
                if code == '0' or code == '1':
                    self.mainQueue.put(('pause', jsoninfo))
                    return 'STORED'
        except Exception as e:
            self.dlog.logger.error('Error@Springboard.pause_stream', exc_info=True)
        return 'NOT_STORED'

    def getStat(self, stream_id):
        invalidstat = {'status':1, 'code':5, 'msg':'invalid stream_id', 'delay':1,
                       'filter_lan':1, 'type':'unknow', 'stream_id':stream_id,
                       'stream_url':'', 'access_key':'', 'createTime':''}
        try:
            if stream_id == 'all':
                stat = list()
                for id in self.shareMonitorDict.keys():
                    radiostat = self.getOneStat_v2(id)
                    stat.append(radiostat)
                return json.dumps({"response":stat})

            ids = stream_id.strip().split(',')
            #ids = list(set(ids))
            if len(ids) > 1:
                stat = list()
                for id in ids:
                    if id in self.shareMonitorDict:
                        radiostat = self.getOneStat_v2(id)
                    else:
                        tmp = invalidstat.copy()
                        tmp['stream_id'] = id
                        radiostat = tmp
                    stat.append(radiostat)
                return json.dumps({"response":stat})
            elif len(ids) == 1:
                if ids[0] in self.shareMonitorDict:
                    radiostat = self.getOneStat_v2(ids[0])
                    return json.dumps({"response":radiostat})
                else:
                    invalidstat['stream_id'] = ids[0]
                    return json.dumps({'response':invalidstat})

        except Exception as e:
            self.dlog.logger.error('Error@@Springboard.get_state', exc_info=True)

        return json.dumps({'response':invalidstat})

    def getOneStat_v2(self, stream_id):
        radiostat = {"status":0, "code":0, "msg":"running", "type":"unknow", "delay":1, "filter_lan":1,
                     "stream_id":stream_id, "stream_url":self.shareMonitorDict[stream_id][0]["stream_url"],
                     "access_key":self.shareMonitorDict[stream_id][0]["access_key"],
                     "createTime":self.shareMonitorDict[stream_id][1]}
        code, msg = self.shareStatusDict[stream_id][0].split("#")
        radiostat["status"] = int(code)
        radiostat["code"] = int(code)
        radiostat["msg"] = msg
        radiostat["delay"] = int(self.shareDict.get('delay_'+stream_id, 1))
        radiostat["record"] = self.shareDict.get('record_'+stream_id, [0,0,0])[0]
        radiostat["record_before"] = self.shareDict.get('record_'+stream_id, [0,0,0])[1]
        radiostat["record_after"] = self.shareDict.get('record_'+stream_id, [0,0,0])[2]
        radiostat["filter_lan"] = self.shareDict.get('filter_chinese_'+stream_id, 1)
        isVideo, ismsg = self.shareStatusDict[stream_id][1].split("#")
        if isVideo == "0":
            radiostat["type"] = "audio"
        elif isVideo == "1":
            radiostat["type"] = "video"
        else:
            radiostat["type"] = "unknow"
        return radiostat

    def stop(self):
        try:
            self.mainQueue.put(('stop', ''))
            return 'STORED'
        except Exception as e:
            self.dlog.logger.error('Error@Springboard.stop_stream', exc_info=True)
        return 'NOT_STORED'

class AcrcloudMonitor:

    def __init__(self, mainQueue, config,
                 shareMonitorDict,
                 shareStatusDict,
                 shareDict,
                 dworker,
                 rworker,
                 sworker):
        self.recQueue = multiprocessing.Queue()
        self.recMainQueue = multiprocessing.Queue()
        self.resultQueue= multiprocessing.Queue()
        self.resMainQueue = multiprocessing.Queue()
        self.springQueue = mainQueue
        self.config  = config
        self.shareMonitorDict = shareMonitorDict
        self.shareStatusDict = shareStatusDict
        self.shareDict = shareDict
        self.procDict = dict()
        self.dworker = dworker
        self.rworker = rworker
        self.sworker = sworker
        self.initLog()
        self.initRec()
        self.initRes()

    def initLog(self):
        self.colorfmt = "$MAGENTA%(asctime)s - $RED%(name)-20s$RESET - $COLOR%(levelname)-8s$RESET - $COLOR%(message)s$RESET"
        self.dlog = AcrcloudLogger('Monitor@Main', logging.INFO)
        if not self.dlog.addFilehandler(logfile = "Monitor.log", logdir = self.config["log"]["dir"]):
            sys.exit(1)
        if not self.dlog.addStreamHandler(self.colorfmt):
            sys.exit(1)

    def initRec(self):
        self.recproc = multiprocessing.Process(target=self.rworker,
                                               args=(self.recMainQueue,
                                                     self.recQueue,
                                                     self.resultQueue,
                                                     self.shareDict,
                                                     self.config))
        self.recproc.start()
        if not self.recproc.is_alive():
            self.dlog.logger.error('Error@AcrcloudMonitor.init_recognize.failed')
            sys.exit(1)
        else:
            self.dlog.logger.warn('Warn@AcrcloudMonitor.init_recognize.success')

    def initRes(self):
        self.resproc = multiprocessing.Process(target=self.sworker,
                                               args=(self.resMainQueue,
                                                     self.resultQueue,
                                                     self.config))
        self.resproc.start()
        if not self.resproc.is_alive():
            self.dlog.logger.error('Error@AcrcloudMonitor.init_result.failed')
            sys.exit(1)
        else:
            self.dlog.logger.warn('Warn@AcrcloudMonitor.init_result.success')

    def checkInfo(self, info):
        if len(info) >= 8:
            for key in self.addkeys:
                if info.get(key, 'None') == 'None':
                    return False
            return True
        return False

    def changeStat(self, id, index, msg):
        stat = self.shareStatusDict[id]
        stat[index] = msg
        self.shareStatusDict[id] = stat

    def startMonitor(self):
        try:
            for stream_id in self.shareMonitorDict.keys():
                jsoninfo = self.shareMonitorDict[stream_id][0]
                self.addMonitor(jsoninfo)
                time.sleep(1)
        except Exception as e:
            self.dlog.logger.error('Error@AcrcloudMonitor.startMonitor', exc_info=True)

    def addMonitor(self, jsoninfo):
        try:
            stream_id = jsoninfo.get('stream_id')
            if stream_id in self.shareMonitorDict and stream_id not in self.procDict:
                mainqueue = multiprocessing.Queue()
                proc = multiprocessing.Process(target=self.dworker,
                                               args=(jsoninfo, mainqueue,
                                                     self.recQueue,
                                                     self.shareStatusDict,
                                                     self.shareDict,
                                                     self.config))
                proc.start()
                if proc.is_alive():
                    self.procDict[stream_id] = [proc, mainqueue]
                    self.dlog.logger.warn('ADD Monitor ({0}, {1})'.format(jsoninfo['stream_id'], jsoninfo['stream_url']))
                    return True
        except Exception as e:
            self.dlog.logger.error('Error@AcrcloudMonitor.ADD Monitor Error:', exc_info=True)
        self.dlog.logger.error('Error@AcrcloudMonitor.ADD Monitor Failed ({0}, {1})'.format(jsoninfo['stream_id'], jsoninfo['stream_url']))
        return False

    def delMonitor(self, jsoninfo):
        try:
            stream_id = jsoninfo.get('stream_id')
            proc, mainqueue = self.procDict[stream_id]
            mainqueue.put('STOP')
            proc.join()
            if not proc.is_alive():
                #del self.shareStatusDict[stream_id]
                self.shareStatusDict[stream_id] = ['10#delete', '2#unknow']
                del self.procDict[stream_id]
                self.dlog.logger.warn('DEL Monitor ({0}, {1})'.format(jsoninfo['stream_id'], jsoninfo['stream_url']))
                return True
        except Exception as e:
            self.dlog.logger.error('Error@AcrcloudMonitor.Del Monitor Error:', exc_info=True)
        self.dlog.logger.error('Error@AcrcloudMonitor.Del Monitor Failed ({0}, {1})'.format(jsoninfo['stream_id'], jsoninfo['stream_url']))
        return False

    def delAllM(self):
        try:
            for stream_id in self.procDict.keys():
                if self.delMonitor({'stream_id':stream_id, 'stream_url':''}):
                    del self.shareMonitorDict[stream_id]
        except Exception as e:
            self.dlog.logger.error('Del All Monitors Error', exc_info=True)

    def reStart(self, jsoninfo):
        try:
            stream_id = jsoninfo.get('stream_id')
            if stream_id in self.shareMonitorDict:
                code, msg = self.shareStatusDict[stream_id][0].split('#')
                proc, mainqueue = self.procDict[stream_id]
                info, createTime = self.shareMonitorDict[stream_id][:2]
                if code == '4' or code == '3' or code == '6':
                    if proc.is_alive():
                        mainqueue.put('RESTART')
                        self.changeStat(stream_id, 0, '0#restart0')
                        self.dlog.logger.warn('Restart Monitor ({0}, {1}).'.format(jsoninfo['stream_id'], jsoninfo['stream_url']))
                        return True
                    else:
                        proc = multiprocessing.Process(target=self.dworker,
                                                       args=(info, mainqueue,
                                                             self.recQueue,
                                                             self.shareStatusDict,
                                                             self.shareDict,
                                                             self.config))
                        proc.start()
                        if proc.is_alive():
                            self.procDict[stream_id][0] = proc
                            self.changeStat(stream_id, 0, '0#restart1')
                            self.dlog.logger.warn('Restart Monitor ({0}, {1}).'.format(jsoninfo['stream_id'], jsoninfo['stream_url']))
                            return True
        except Exception as e:
            self.dlog.logger.error('Error@AcrcloudMonitor.Restart Monitor Error:', exc_info=True)
        self.dlog.logger.error('Erro@AcrcloudMonitor.Restart Monitor Failed ({0}, {1}).'.format(jsoninfo['stream_id'], jsoninfo['stream_url']))
        return False

    def refresh(self):
        try:
            for stream_id in self.shareMonitorDict.keys():
                jsoninfo, createTime, value = self.shareMonitorDict[stream_id]
                if value == 1:
                    self.dlog.logger.warn('Warn@AcrcloudMonitor.refresh.stream: {0} - Update'.format(jsoninfo.get('stream_id','')))
                    self.delMonitor(jsoninfo)
                    self.addMonitor(jsoninfo)
                elif value == 2:
                    self.dlog.logger.warn('Warn@AcrcloudMonitor.refresh.stream: {0} - New Add'.format(jsoninfo.get('stream_id','')))
                    self.addMonitor(jsoninfo)
                elif value == 3:
                    self.dlog.logger.warn('Warn@AcrcloudMonitor.refresh.stream: {0} - Delete'.format(jsoninfo.get('stream_id','')))
                    self.delMonitor(jsoninfo)
                    del self.shareMonitorDict[stream_id]
                time.sleep(1)
        except Exception as e:
            self.dlog.logger.error('Error@AcrcloudMonitor.Refresh Monitor Error:', exc_info=True)

    def pauseM(self, jsoninfo):
        try:
            stream_id = jsoninfo.get('stream_id')
            if stream_id in self.shareMonitorDict:
                code, msg = self.shareStatusDict[stream_id][0].split('#')
                if code == '0' or code == '1':
                    proc, mainqueue = self.procDict[stream_id]
                    mainqueue.put('PAUSE')
                    self.dlog.logger.warn('PAUSE Monitor ({0}, {1}).'.format(jsoninfo['stream_id'], jsoninfo['stream_url']))
                    return True
        except Exception as e:
            self.dlog.logger.error('Error@AcrcloudMonitor.PAUSE Monitor Error:', exc_info=True)
        self.dlog.logger.error('Error@AcrcloudMonitor.PAUSE Monitor Failed ({0}, {1}).'.format(jsoninfo['stream_id'], jsoninfo['stream_url']))
        return False

    def doIt(self, cmd, info):
        try:
            if cmd == 'heiheihei':
                self.startMonitor()
            elif cmd == 'refresh':
                self.refresh()
            elif cmd == 'restart':
                self.reStart(info)
            elif cmd == 'pause':
                self.pauseM(info)
            elif cmd == 'stop':
                self.stop()
        except Exception as e:
            self.dlog.logger.error("doIt Error:", exc_info=True)

    def start(self):
        self._running = True
        while 1:
            if not self._running:
                break
            try:
                cmd, info = self.springQueue.get()
            except Queue.Empty:
                continue
            self.doIt(cmd, info)

    def stop(self):
        self.delAllM()
        self.dlog.logger.warn('Warn@Acrcloud_Manager.DelAllMontirs_Success')
        self.recMainQueue.put(('stop',''))
        self.resMainQueue.put(('stop',''))
        self._running = False
        self.dlog.logger.warn('Warn@Acrcloud_Manager_Stop')
        sys.exit(1)

def MonitorManager(mainQueue, config, shareMonitorDict, shareStatusDict, shareDict, dworker, rworker, sworker):
    mManager = AcrcloudMonitor(mainQueue, config, shareMonitorDict, shareStatusDict, shareDict, dworker, rworker, sworker)
    mManager.start()

def RadioWorker(info, queue, recqueue, shareStatusDict, shareDict, config):
    iWorker = AcrcloudWorker(info, queue, recqueue, shareStatusDict, shareDict, config)
    iWorker.start()

def RecWorker(mainqueue, recqueue, resultqueue, shareDict, config):
    rWorker = Acrcloud_Rec_Manager(mainqueue, recqueue, resultqueue, shareDict, config)
    rWorker.start()

def ResWorker(mainqueue, resultqueue, config):
    sWorker = Acrcloud_Result(mainqueue, resultqueue, config)
    sWorker.start()

acrcloudMana = AcrcloudManager(AcrcloudSpringboard(MonitorManager, config, RadioWorker, RecWorker, ResWorker))

