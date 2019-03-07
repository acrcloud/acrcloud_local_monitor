#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys
import math
import json
import copy
import time
import Queue
import random
import MySQLdb
import datetime
import threading
import traceback
import tools_str_sim
import tools_language
import multiprocessing
from dateutil.relativedelta import *
from acrcloud_record import recordWorker
from acrcloud_callback import postManager
from tools_data_filter_local import ResultFilter

reload(sys)
sys.setdefaultencoding("utf8")

sys.path.append("../")
import acrcloud_logger

NORESULT = "noResult"

class Acrcloud_Result:
    def __init__(self, mainqueue, resultQueue, config):
        self._config = config
        self._log_dir = self._config['log']['dir']
        self._mainQueue = mainqueue
        self._resultQueue = resultQueue
        self._callbackQueue = multiprocessing.Queue()
        self._recordQueue = multiprocessing.Queue()
        self.init_logger()
        self.data_backup = Backup(self._config, self._recordQueue, self._callbackQueue, self.dlog)
        self.init_record()
        self.init_callback_manager()
        self.dlog.logger.warn("Warn@Acrcloud_Result Init Success!")

    def init_logger(self):
        self.dlog = acrcloud_logger.AcrcloudLogger('LocalResult')
        self.dlog.addFilehandler('local_result.log', logdir=self._log_dir)
        self.dlog.addStreamHandler()

    def init_record(self):
        try:
            self.record_proc = multiprocessing.Process(target = recordWorker,
                                                       args = (self._config, self._recordQueue))
            self.record_proc.daemon = True
            self.record_proc.start()
            if not self.record_proc.is_alive():
                self.dlog.logger.error("Error@Acrcloud_Result.record worker run failed, exit")
                sys.exit(1)
            else:
                self.dlog.logger.warn("Warn@Acrcloud_Result.init_record success")
        except Exception as e:
            self.dlog.logger.error('Error@Acrcloud_Result.init_record', exc_info=True)

    def init_callback_manager(self):
        try:
            self.callback_manager_proc = multiprocessing.Process(target=postManager, args=(self._callbackQueue,))
            self.callback_manager_proc.daemon = True
            self.callback_manager_proc.start()
            if not self.callback_manager_proc.is_alive():
                self.dlog.logger.error("Error@Acrcloud_Result.init_callback_manager run failed, exit")
                sys.exit(1)
            else:
                self.dlog.logger.warn("Warn@Acrcloud_Result.init_callback_manager success")
        except Exception as e:
            self.dlog.logger.error("Error@Acrcloud_Result.init_callback_manager", exc_info=True)


    def deal_result(self, resinfo):
        try:
            self.data_backup.save_one(resinfo)
        except Exception as e:
            self.dlog.logger.error('Error@Acrcloud_Result.deal_result', exc_info=True)

    def start(self):
        self._running = True
        while 1:
            if not self._running:
                break
            try:
                maininfo = self._mainQueue.get(block = False)
                if maininfo and maininfo[0] == 'stop':
                    self.stop()
            except Queue.Empty:
                time.sleep(0.01)

            try:
                resinfo = self._resultQueue.get(block = False)
            except Queue.Empty:
                if random.random() < 0.1:
                    time.sleep(0.02)
                continue

            self.deal_result(resinfo)

            if random.random() < 0.1:
                self.dlog.logger.warn("Warn@resultQueue.qsize.{0}".format(self._resultQueue.qsize()))

    def stop(self):
        self._running = False
        self.dlog.logger.warn('Warn@Acrcloud_Result_Stop')
        sys.exit(1)



class Backup:

    def __init__(self, config, recordQueue, callbackQueue, dlog):
        self._sql = "insert into result_info (access_key, stream_url, stream_id, result, timestamp, catchDate) values (%s, %s, %s, %s, %s, CURRENT_DATE()) on duplicate key update id=LAST_INSERT_ID(id)"
        self._config = config
        db_config = self._config["database"]
        self._recordQueue = recordQueue
        self._callbackQueue = callbackQueue
        self._redis_map = {}
        self.dlog = dlog
        self._mdb = MysqlManager(host=db_config["host"],
                                 port=db_config["port"],
                                 user=db_config["user"],
                                 passwd=db_config["passwd"],
                                 dbname=db_config["db"])
        self._log_dir = self._config["log"]["dir"]
        self._result_filter = ResultFilter(self.dlog, self._log_dir)
        self._tools_lan = tools_language.tools_language()

    def filter_chinese(self, stream_id, result):
        try:
            if not result:
                return ""
            if "metainfos" in result:
                return result
            if "metadata" in result and "custom_files" in result["metadata"]:
                return result

            music = result["metadata"]["music"][0]
            if self._tools_lan.has_chinese(music["title"]):
                self.dlog.logger.warn("Lan@StreamID: {0}, Title Has Chinese: {1}".format(stream_id,music["title"]))
                return ""
            if self._tools_lan.has_chinese(music["artists"][0]["name"]):
                self.dlog.logger.warn("Lan@StreamID: {0}, Artists Has Chinese: {1}".format(stream_id,music["artists"][0]["name"]))
                return ""
            if music.get("label") and self._tools_lan.has_chinese(music["label"]):
                result["metadata"]["music"][0]["label"] = ""
            if self._tools_lan.has_chinese(music["album"]["name"]):
                result["metadata"]["music"][0]["album"]["name"] = ""
            return result
        except Exception as e:
            self.dlog.logger.error("Error@filter_chinese, error data:{0}".format(json.dumps(result)), exc_info=True)
        return result

    def format_timestamp(self, timestr):
        return datetime.datetime.strptime(timestr, "%Y-%m-%d %H:%M:%S").strftime('%Y%m%d%H%M%S')

    def save_one_uniq(self, old_data, isCustom=0):
        filter_chinese = old_data.get("filter_chinese", 0)
        if 'response' in old_data['result']:
            old_data['result'] = old_data['result']['response']

        #从pem_file中清除pem_file
        old_data['pem_file'] = ''

        data = None
        if isCustom:
            data, is_new = self._result_filter.deal_real_custom(old_data)
        else:
            data, is_new = self._result_filter.deal_real_history(old_data)
        if data is None:
            return False

        access_key = data.get('access_key', 'unknow')
        stream_url = data.get('stream_url', 'unknow')
        stream_id = data.get('stream_id', 'unknow')

        if isCustom:
            title = self._result_filter.get_mutil_result_acrid(data, 'custom')[0]
        else:
            title = self._result_filter.get_mutil_result_title(data, 'music', 1)[0]

        if isCustom:
            redis_name = "monitor_custom_{0}_{1}".format(access_key, stream_id)
        else:
            redis_name = "monitor_{0}_{1}".format(access_key, stream_id)
        if redis_name not in self._redis_map:
            self._redis_map[redis_name] = {'title':'', 'info': ''}

        redis_info = self._redis_map[redis_name]
        redis_title = redis_info['title']
        if title == NORESULT:
            pass

        if redis_title != title :
            self._redis_map[redis_name]['title'] = title
            self._redis_map[redis_name]['info'] = json.dumps(data)

        if is_new and NORESULT != title:
            result = data.get('result', {})
            if result and filter_chinese:
                result = self.filter_chinese(stream_id, result)

            if self._mdb and result:
                data['result'] = result

                #post result to callback url
                try:
                    postdata = copy.deepcopy(data)
                    self._callbackQueue.put(json.dumps(postdata))
                except Exception as e:
                    self.dlog.logger.error("Error@save_one_uniq.send_to_callback", exc_info=True)

                params = (access_key,
                          stream_url,
                          stream_id,
                          json.dumps(result),
                          data.get('timestamp'),)
                try:
                    self._mdb.execute(self._sql, params)
                    self._mdb.commit()
                    return True
                except MySQLdb.Error as e:
                    self.dlog.logger.error("Error@save_one_uniq.db_execute", exc_info=True)
        return False

    def save_one_delay(self, old_data, isCustom=0):
        filter_chinese = old_data.get("filter_chinese", 0)
        if 'response' in old_data['result']:
            old_data['result'] = old_data['result']['response']

        if (isCustom==0 and old_data.get('record', [0,0,0])[0] in [2,3]) or (isCustom==1 and old_data.get('record', [0,0,0])[0]==1):
            record_add_data = copy.deepcopy(old_data)
            self._recordQueue.put(('add', record_add_data))

        #从pem_file中清除pem_file
        old_data['pem_file'] = ''

        data = None
        if isCustom:
            data = self._result_filter.deal_delay_custom(old_data)
        else:
            data = self._result_filter.deal_delay_history(old_data)
        if data is None:
            return False

        access_key = data.get('access_key', 'unknow')
        stream_url = data.get('stream_url', 'unknow')
        stream_id = data.get('stream_id', 'unknow')

        result = data.get('result', {})
        if result and filter_chinese:
            result = self.filter_chinese(stream_id, result)
        if self._mdb and result:
            data['result'] = result  #之前涉及到的拷贝都是浅拷贝，这里的修改会影响到浅拷贝的变量history
            try:
                if (isCustom==0 and old_data.get('record', [0,0,0])[0] in [2,3]) or (isCustom==1 and old_data.get('record', [0,0,0])[0] in [1,3]):
                    self._recordQueue.put(('save', data))
                    result['metadata']['record_timestamp'] = self.format_timestamp(data['timestamp'])
                    self.dlog.logger.info("Send To Record (streamID:{0}, isCustom:{1}, record:{2})".format(stream_id, isCustom, ",".join([ str(i) for i in data.get('record', [0,0,0])])))
            except Exception as e:
                self.dlog.logger.error("Error@save_one_delay.record.save_audio", exc_info=True)

            #post result to callback url
            try:
                postdata = copy.deepcopy(data)
                self._callbackQueue.put(json.dumps(postdata))
            except Exception as e:
                self.dlog.logger.error("Error@save_one_uniq.send_to_callback", exc_info=True)

            params = (access_key,
                      stream_url,
                      stream_id,
                      json.dumps(result),
                      data.get('timestamp'),)
            try:
                self._mdb.execute(self._sql, params)
                self._mdb.commit()
                return True
            except MySQLdb.Error as e:
                self.dlog.logger.error("Error@save_one_delay.db_execute", exc_info=True)
        return False

    def save_one(self, jsondata):
        try:
            timestamp = jsondata['timestamp']
            if jsondata['result']['status']['code'] != 0:
                jsondata['result']['metadata'] = {'timestamp_utc':timestamp}
            elif 'metadata' in jsondata['result']:
                jsondata['result']['metadata']['timestamp_utc'] = timestamp

            ret = False
            if jsondata.get('delay') == 0 or jsondata.get('delay') == False:
                custom_data = copy.deepcopy(jsondata)
                if jsondata['result']['status']['code'] != 0:
                    ret = self.save_one_uniq(jsondata, 0)
                    ret = self.save_one_uniq(custom_data, 1)
                elif 'metadata' in jsondata['result'] and 'custom_files' in jsondata['result']['metadata']:
                    if 'music' in jsondata['result']['metadata']:
                        del custom_data['result']['metadata']['music']
                        del jsondata['result']['metadata']['custom_files']
                        ret = self.save_one_uniq(jsondata, 0)
                    else:
                        jsondata['result'] = {u'status': {u'msg': u'No result', u'code': 1001, u'version': u'1.0'}}
                        ret = self.save_one_uniq(jsondata, 0)
                    ret = self.save_one_uniq(custom_data, 1)
                elif 'metadata' in jsondata['result'] and 'music' in jsondata['result']['metadata']:
                    custom_data['result'] = {u'status': {u'msg': u'No result', u'code': 1001, u'version': u'1.0'}}
                    ret = self.save_one_uniq(custom_data, 1)
                    ret = self.save_one_uniq(jsondata, 0)

            elif jsondata.get('delay') == 1 or jsondata.get('delay') == True:
                custom_data = copy.deepcopy(jsondata)
                if jsondata['result']['status']['code'] != 0:
                    ret = self.save_one_delay(jsondata, 0)
                    ret = self.save_one_delay(custom_data, 1)
                elif 'metadata' in jsondata['result'] and 'custom_files' in jsondata['result']['metadata']:
                    if 'music' in jsondata['result']['metadata']:
                        del custom_data['result']['metadata']['music']
                        del jsondata['result']['metadata']['custom_files']
                        ret = self.save_one_delay(jsondata, 0)
                    else:
                        jsondata['result'] = {u'status': {u'msg': u'No result', u'code': 1001, u'version': u'1.0'}}
                        ret = self.save_one_delay(jsondata, 0)
                    ret = self.save_one_delay(custom_data, 1)
                elif 'metadata' in jsondata['result'] and 'music' in jsondata['result']['metadata']:
                    custom_data['result'] = {u'status': {u'msg': u'No result', u'code': 1001, u'version': u'1.0'}}
                    ret = self.save_one_delay(custom_data, 1)
                    ret = self.save_one_delay(jsondata, 0)
            if ret:
                self.dlog.logger.info("MSG@Result_Save_Success({0})".format(jsondata.get("stream_id")))
        except Exception as e:
            self.dlog.logger.error("Error@save_one", exc_info=True)
            self.dlog.logger.error("Error_Data:{0}, {1}".format(jsondata.get('stream_id'), jsondata.get('result')))
        return ret


class MysqlManager:
    def __init__(self, host, port, user, passwd, dbname):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.dbname = dbname
        self.conn = MySQLdb.connect(host=host, user=user,
                                    passwd=passwd, db=dbname,
                                    port=port, charset="utf8")
        #self.conn = MySQLdb.connect(host, port, user, passwd, dbname, charset="utf8")
        self.curs = self.conn.cursor()

    def reconnect(self):
        self.conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user,
                passwd=self.passwd, db=self.dbname, charset="utf8")
        self.curs = self.conn.cursor()

    def commit(self):
        try:
            self.conn.commit();
        except (AttributeError, MySQLdb.Error):
            self.reconnect()
            try:
                self.conn.commit();
            except MySQLdb.Error:
                raise

    def execute(self, sql, params=None):
        if params:
            try:
                self.curs.execute(sql, params)
            except (AttributeError, MySQLdb.Error):
                self.reconnect()
                try:
                    self.curs.execute(sql, params)
                except MySQLdb.Error:
                    raise
        else:
            try:
                self.curs.execute(sql)
            except (AttributeError, MySQLdb.Error):
                self.reconnect()
                try:
                    self.curs.execute(sql)
                except MySQLdb.Error:
                    raise
        return self.curs

    def executemany(self, sql, params):
        if params:
            try:
                self.curs.executemany(sql, params)
            except (AttributeError, MySQLdb.Error):
                self.reconnect()
                try:
                    self.curs.executemany(sql, params)
                except MySQLdb.Error:
                    raise
        return self.curs
