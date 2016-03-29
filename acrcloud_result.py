#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys
import json
import copy
import time
import redis
import MySQLdb
import datetime
import threading
import traceback
import tools_str_sim
import tools_language
import multiprocessing
reload(sys)
sys.setdefaultencoding("utf8")

sys.path.append("../")
import acrcloud_logger


NORESULT = "noResult"


class Acrcloud_Result:
    def __init__(self, resultQueue, config):
        self._config = config
        self._log_dir = self._config['log']['dir']
        self._resultQueue = resultQueue
        self.init_logger()
        self.data_backup = Backup(self._config, self.dlog)
        self.dlog.logger.info("Acrcloud_Result Init Success!")
        
    def init_logger(self):
        self.dlog = acrcloud_logger.AcrcloudLogger('LocalResult')
        self.dlog.addFilehandler('local_result.log', logdir=self._log_dir)
        self.dlog.addStreamHandler()
            
    def deal_result(self, resinfo):
        try:
            self.data_backup.save_one(resinfo)
        except Exception as e:
            self.dlog.logger.error('Error@Acrcloud_Result.deal_result', exc_info=True)
            
    def start(self):
        self._running = True
        while 1:
            try:
                resinfo = self._resultQueue.get()
            except Queue.Empty:
                continue
            
            self.deal_result(resinfo)


class ResultFilter:
    def __init__(self, dlog):
        self._dlog = dlog
        self._history_num = 3
        self._history = {}
        self._history_custom = {}
        self._delay_history = {}
        self._delay_custom = {}
        self._delay_list_max_num = 30
        self._delay_list_threshold = self._delay_list_max_num*3

    def checkSame(self, curr_title, stream_id):
        self._history[stream_id] = self._history.get(stream_id, [[''], ''])
        if len(self._history[stream_id][0]) > self._history_num:
            self._history[stream_id][0] = self._history[stream_id][0][-self._history_num:]
            his_max = self._history_num
        else:
            his_max = len(self._history[stream_id][0])
        for i in range(his_max-1, -1, -1):
            if self.checkResultSim(i, curr_title, self._history[stream_id][0][i], stream_id):
            #if self.checkResult(curr_title.lower(), self._history[stream_id][0][i].lower()):
                return True
            if curr_title == NORESULT:
                break
        return False

    def checkSame_custom(self, curr_title, stream_id):
        self._history_custom[stream_id] = self._history_custom.get(stream_id, [[''], ''])
        if len(self._history_custom[stream_id][0]) > self._history_num:
            self._history_custom[stream_id][0] = self._history_custom[stream_id][0][-self._history_num:]
            his_max = self._history_num
        else:
            his_max = len(self._history_custom[stream_id][0])
        for i in range(his_max-1, -1, -1):
            if self.checkResultSim(i, curr_title, self._history_custom[stream_id][0][i], stream_id):
                return True
            if curr_title == NORESULT:
                break
        return False


    def checkResult(self, curr_title, his_title):
        if curr_title.strip() == his_title.strip():
            return True
        if not curr_title or not his_title:
            return False
        for item in ['(', '（']:
            currindex = curr_title.find(item)
            if currindex != -1:
                curr_title = curr_title[:currindex].strip()
            hisindex = his_title.find(item)
            if hisindex != -1:
                his_title = his_title[:hisindex].strip()

        if curr_title == his_title or curr_title.find(his_title)!=-1 or his_title.find(curr_title)!=-1:
            return True
        else:
            return False

    def checkResultSim(self, idx, curr_title, his_title, stream_id):
        if not curr_title or not his_title:
            return False
        sim, detail = tools_str_sim.str_sim(curr_title, his_title)
        if not sim and curr_title != NORESULT and his_title != NORESULT:
            self._dlog.logger.info("Sim@StreamID: {0}, CurrTitle: {1}, HisTitle: {2}({3}), Sim: {4}".format(str(stream_id), curr_title, his_title, str(idx), str(detail)))
        return sim

    def getResultTitle(self, result):
        json_res = result["result"]
        if json_res == NORESULT:
            return NORESULT
        try:
            if json_res['status']['code'] == 0:
                metainfos = json_res.get("metainfos")
                metadata = json_res.get("metadata")
                if metainfos:
                    curr_title = metainfos[0]['title']
                else:
                    if metadata.get('music'):
                        curr_title = metadata['music'][0]['title']
                    else:
                        if metadata['custom_files']:
                            curr_title = metadata['custom_files'][0]['title']
                        else:
                            curr_title = NORESULT
            else:
                curr_title = NORESULT
        except Exception as e:
            self._dlog.logger.error("Error@getResultTitle", exc_info=True)
            self._dlog.logger.error("Error_Data: {0}".format(json.dumps(result)))
            return NORESULT
        return curr_title

    def updateResultTitle(self, data, new_title):
        if new_title == NORESULT:
            return
        try:
            json_res = data["result"]
            metainfos = json_res.get("metainfos")
            metadata = json_res.get("metadata")
            if metainfos:
                metainfos[0]['title'] = new_title
            else:
                if metadata.get('music'):
                    metadata['music'][0]['title'] = new_title
                else:
                    metadata['custom_files'][0]['title'] = new_title
        except Exception as e:
            self._dlog.logger.error("Error@updateResultTitle", exc_info=True)

    def tryStrSub(self, try_str):
        sub_str = tools_str_sim.str_sub(try_str)
        if len(sub_str) > 0 and len(try_str) > len(sub_str):
            return sub_str, True
        return try_str, False

    def tryUpdateResultTitle(self, data):
        title = self.getResultTitle(data)
        stream_id = data.get("stream_id")
        new_title, try_status = self.tryStrSub(title)
        if try_status:
            self.updateResultTitle(data, new_title)
            self._dlog.logger.info("StreamID: {0}, Update Title: [{1}] >>> [{2}]".format(stream_id, title, new_title))
            return new_title
        return title

    def handleResult(self, result):
        curr_title = self.getResultTitle(result)
        stream_id = result.get("stream_id")
        if not stream_id:
            return None
        if curr_title == NORESULT:
            if not self.checkSame(curr_title, stream_id):
                self._history[stream_id][0].append(curr_title)
                self._history[stream_id][1] = result
            else:
                result = None
        else:
            if self.checkSame(curr_title, stream_id):
                result = self._history[stream_id][1]
            else:
                self._history[stream_id][0].append(curr_title)
                self._history[stream_id][1] = result
        return result

    def handleResult_custom(self, result):
        curr_title = self.getResultTitle(result)
        stream_id = result.get("stream_id")
        if not stream_id:
            return None
        if curr_title == NORESULT:
            if not self.checkSame_custom(curr_title, stream_id):
                self._history_custom[stream_id][0].append(curr_title)
                self._history_custom[stream_id][1] = result
            else:
                result = None
        else:
            if self.checkSame_custom(curr_title, stream_id):
                result = self._history_custom[stream_id][1]
            else:
                self._history_custom[stream_id][0].append(curr_title)
                self._history_custom[stream_id][1] = result
        return result
    
    def getDuration(self, end_timestamp, start_timestamp):
        try:
            duration = 0
            end = datetime.datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S')
            start = datetime.datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S')
            duration = (end - start).seconds
        except Exception as e:
            self._dlog.logger.error("Error@getDuration", exc_info=True)
        return duration
    
    def delay_deal_custom(self, data):
        stream_id = data.get("stream_id")        
        timestamp = data.get("timestamp")
        raw_title = self.getResultTitle(data)
        if stream_id not in self._delay_custom:
            self._delay_custom[stream_id] = [(raw_title, raw_title, timestamp, data)]
        else:
            self._delay_custom[stream_id].append((raw_title, raw_title, timestamp, data))
            
        if len(self._delay_custom[stream_id]) > self._delay_list_max_num :
            return self.runDelayX_custom(stream_id)
        else:
            return None

    def runDelayX_custom(self, stream_id):
        history_data = self._delay_custom[stream_id]
        if len(history_data) >= self._delay_list_threshold:
            self._dlog.logger.error("delay_custom[{0}] list num({1}) over threshold {2}".format(stream_id, len(history_data), self._delay_list_threshold))
            self._dlog.logger.error("delay_custom[{0}] data: \n{1}".format(stream_id, '\n'.join([str(item[:-1]) for item in history_data])))
            history_data = history_data[-self._delay_list_max_num:]
        
        sim_title_set = set()
        sim_title_count = {}
        #sim_title_list = [ item[1] for item in history_data ]
        for index, item in enumerate(history_data):
            if item[1] not in sim_title_set:
                sim_title_count[item[1]] = [1, [index, ]]
                sim_title_set.add(item[1])
            else:
                sim_title_count[item[1]][0] += 1
                sim_title_count[item[1]][1].append(index)
        sim_title_count_single_index = [sim_title_count[key][1][0] for key in sim_title_count if sim_title_count[key][0] == 1]

        #如果最后一个为单数的则将其从单数列表中删除
        if len(history_data)-1 in sim_title_count_single_index:
            sim_title_count_single_index.remove(len(history_data)-1)
        
        deal_num = 3 #将noresult和单个count的index添加到del_index
        
        del_index = set()
        order_key_list = []
        order_set = set()
        for index, item in enumerate(history_data):
            if item[1] == NORESULT:
                del_index.add(index)
                continue
            if item[1] in order_set:
                continue
            order_key_list.append(item[1])
            order_set.add(item[1])
            deal_num -= 1
            if deal_num <= 0:
                break

        retdata = None
        duration = 0
        another_del_index = set()

        if len(order_key_list) == 3:
            first_item = sim_title_count[order_key_list[0]]
            second_item = sim_title_count[order_key_list[1]]
            third_item = sim_title_count[order_key_list[2]]
            #判断这三者在时间上是否有交集，只是判断第一个和后两个是否有交集
            xflag = 0 #0第一个和后两个无交集，1第一个只和第二个有交集，2第一个和后两个都有交集
            if first_item[1][-1] < second_item[1][0]:
                xflag = 0 if first_item[1][-1] < third_item[1][0] else 2
            else:
                if first_item[1][-1] < third_item[1][0]:
                    xflag = 1 if second_item[1][-1] < third_item[1][0] else 2
                else:
                    xflag = 2
            if xflag == 0:
                retdata = history_data[first_item[1][0]][-1]
                duration = self.getDuration(history_data[first_item[1][-1]][2], history_data[first_item[1][0]][2])
                another_del_index = set(first_item[1])
            elif xflag == 1:
                lucky_item = first_item if first_item[0] >= second_item[0] else second_item
                retdata = history_data[lucky_item[1][0]][-1]
                start_index = first_item[1][0] if first_item[1][0] < second_item[1][0] else second_item[1][0]
                end_index = first_item[1][-1] if first_item[1][-1] > second_item[1][-1] else second_item[1][-1]
                duration = self.getDuration(history_data[end_index][2], history_data[start_index][2])
                another_del_index = set(first_item[1])
                another_del_index = another_del_index.union(set(second_item[1]))
            elif xflag == 2:
                lucky_item = first_item if first_item[0] >= second_item[0] else second_item
                lucky_item = lucky_item if lucky_item[0] >= third_item[0] else third_item
                retdata = history_data[lucky_item[1][0]][-1]
                start_index = min(first_item[1][0], second_item[1][0], third_item[1][0])
                end_index = max(first_item[1][-1], second_item[1][-1], third_item[1][-1])
                duration = self.getDuration(history_data[end_index][2], history_data[start_index][2])
                another_del_index = set(first_item[1])
                another_del_index = another_del_index.union(set(second_item[1]))
                another_del_index = another_del_index.union(set(third_item[1]))
        elif len(order_key_list) == 2:
            first_item = sim_title_count[order_key_list[0]]
            second_item = sim_title_count[order_key_list[1]]
            if first_item[1][-1] < second_item[1][0]:
                retdata = history_data[first_item[1][0]][-1]
                start_index = first_item[1][0]
                end_index = first_item[1][-1]
                duration = self.getDuration(history_data[end_index][2], history_data[start_index][2])
                another_del_index = set(first_item[1])
            else:
                lucky_item = first_item if first_item[0] >= second_item[0] else second_item
                retdata = history_data[lucky_item[1][0]][-1]
                start_index = min(first_item[1][0], second_item[1][0])
                end_index = max(first_item[1][-1], second_item[1][-1])
                duration = self.getDuration(history_data[end_index][2], history_data[start_index][2])
                another_del_index = set(first_item[1])
                another_del_index = another_del_index.union(set(second_item[1]))
        elif len(order_key_list) == 1:
            retdata = history_data[sim_title_count[order_key_list[0]][1][0]][-1]
            start_index = sim_title_count[order_key_list[0]][1][0]
            end_index = sim_title_count[order_key_list[0]][1][-1]
            duration = self.getDuration(history_data[end_index][2], history_data[start_index][2])
            another_del_index = set(sim_title_count[order_key_list[0]][1])


        if another_del_index and max(another_del_index) < len(history_data)-1:
            del_index = del_index.union(another_del_index)
        else:
            retdata = None
            
        after_del_history_data = []
        for index in range(len(history_data)):
            if index not in del_index:
                after_del_history_data.append(history_data[index])
        
        self._delay_custom[stream_id] = after_del_history_data
        
        if retdata:
            self.tryUpdateResultTitle(retdata)
            retdata['result']['metadata']['played_duration'] = duration + 20

        return retdata
        
        
    def delay_deal(self, data):
        stream_id = data.get("stream_id")        
        timestamp = data.get("timestamp")
        raw_title = self.getResultTitle(data)
        sim_title = self.tryStrSub(raw_title)
        if stream_id not in self._delay_history:
            self._delay_history[stream_id] = [(raw_title, sim_title[0], timestamp, data)]
        else:
            self._delay_history[stream_id].append((raw_title, sim_title[0], timestamp, data))

        if len(self._delay_history[stream_id]) > self._delay_list_max_num :
            return self.runDelayX(stream_id)
        else:
            return None
        
    def runDelayX(self, stream_id):
        history_data = self._delay_history[stream_id]
        if len(history_data) >= self._delay_list_threshold:
            self._dlog.logger.error("delay_history[{0}] list num({1}) over threshold {2}".format(stream_id, len(history_data), self._delay_list_threshold))
            self._dlog.logger.error("delay_history[{0}] data: \n{1}".format(stream_id, '\n'.join([str(item[:-1]) for item in history_data])))
            history_data = history_data[-self._delay_list_max_num:]
        
        sim_title_set = set()
        sim_title_count = {}
        #sim_title_list = [ item[1] for item in history_data ]
        for index, item in enumerate(history_data):
            if item[1] not in sim_title_set:
                sim_title_count[item[1]] = [1, [index, ]]
                sim_title_set.add(item[1])
            else:
                sim_title_count[item[1]][0] += 1
                sim_title_count[item[1]][1].append(index)
        sim_title_count_single_index = [sim_title_count[key][1][0] for key in sim_title_count if sim_title_count[key][0] == 1]

        #如果最后一个为单数的则将起从单数列表中删除
        if len(history_data)-1 in sim_title_count_single_index:
            sim_title_count_single_index.remove(len(history_data)-1)
        
        deal_num = 3 #将noresult和单个count的index添加到del_index
        
        del_index = set()
        order_key_list = []
        order_set = set()
        for index, item in enumerate(history_data):
            if item[1] == NORESULT:
                del_index.add(index)
                continue
            if index not in sim_title_count_single_index:
                if item[1] in order_set:
                    continue
                order_key_list.append(item[1])
                order_set.add(item[1])
                index_list = sim_title_count[item[1]][1]
                for single_index in sim_title_count_single_index:
                    if single_index not in del_index:
                        if single_index < index_list[-1]:
                            del_index.add(single_index)
                        elif single_index > index_list[-1]:
                            break
                deal_num -= 1
                if deal_num <= 0:
                    break
            else:
                del_index.add(index)

        retdata = None
        duration = 0
        another_del_index = set()

        if len(order_key_list) == 3:
            first_item = sim_title_count[order_key_list[0]]
            second_item = sim_title_count[order_key_list[1]]
            third_item = sim_title_count[order_key_list[2]]
            #判断这三者在时间上是否有交集，只是判断第一个和后两个是否有交集
            xflag = 0 #0第一个和后两个无交集，1第一个只和第二个有交集，2第一个和后两个都有交集
            if first_item[1][-1] < second_item[1][0]:
                xflag = 0 if first_item[1][-1] < third_item[1][0] else 2
            else:
                if first_item[1][-1] < third_item[1][0]:
                    xflag = 1 if second_item[1][-1] < third_item[1][0] else 2
                else:
                    xflag = 2
            if xflag == 0:
                retdata = history_data[first_item[1][0]][-1]
                duration = self.getDuration(history_data[first_item[1][-1]][2], history_data[first_item[1][0]][2])
                another_del_index = set(first_item[1])
            elif xflag == 1:
                lucky_item = first_item if first_item[0] >= second_item[0] else second_item
                retdata = history_data[lucky_item[1][0]][-1]
                start_index = first_item[1][0] if first_item[1][0] < second_item[1][0] else second_item[1][0]
                end_index = first_item[1][-1] if first_item[1][-1] > second_item[1][-1] else second_item[1][-1]
                duration = self.getDuration(history_data[end_index][2], history_data[start_index][2])
                another_del_index = set(first_item[1])
                another_del_index = another_del_index.union(set(second_item[1]))
            elif xflag == 2:
                lucky_item = first_item if first_item[0] >= second_item[0] else second_item
                lucky_item = lucky_item if lucky_item[0] >= third_item[0] else third_item
                retdata = history_data[lucky_item[1][0]][-1]
                start_index = min(first_item[1][0], second_item[1][0], third_item[1][0])
                end_index = max(first_item[1][-1], second_item[1][-1], third_item[1][-1])
                duration = self.getDuration(history_data[end_index][2], history_data[start_index][2])
                another_del_index = set(first_item[1])
                another_del_index = another_del_index.union(set(second_item[1]))
                another_del_index = another_del_index.union(set(third_item[1]))
        elif len(order_key_list) == 2:
            first_item = sim_title_count[order_key_list[0]]
            second_item = sim_title_count[order_key_list[1]]
            if first_item[1][-1] < second_item[1][0]:
                retdata = history_data[first_item[1][0]][-1]
                start_index = first_item[1][0]
                end_index = first_item[1][-1]
                duration = self.getDuration(history_data[end_index][2], history_data[start_index][2])
                another_del_index = set(first_item[1])
            else:
                lucky_item = first_item if first_item[0] >= second_item[0] else second_item
                retdata = history_data[lucky_item[1][0]][-1]
                start_index = min(first_item[1][0], second_item[1][0])
                end_index = max(first_item[1][-1], second_item[1][-1])
                duration = self.getDuration(history_data[end_index][2], history_data[start_index][2])
                another_del_index = set(first_item[1])
                another_del_index = another_del_index.union(set(second_item[1]))
        elif len(order_key_list) == 1:
            retdata = history_data[sim_title_count[order_key_list[0]][1][0]][-1]
            start_index = sim_title_count[order_key_list[0]][1][0]
            end_index = sim_title_count[order_key_list[0]][1][-1]
            duration = self.getDuration(history_data[end_index][2], history_data[start_index][2])
            another_del_index = set(sim_title_count[order_key_list[0]][1])

        if another_del_index and max(another_del_index) < len(history_data)-1:
            del_index = del_index.union(another_del_index)
        else:
            retdata = None
            
        after_del_history_data = []
        for index in range(len(history_data)):
            if index not in del_index:
                after_del_history_data.append(history_data[index])
        
        self._delay_history[stream_id] = after_del_history_data
        
        if retdata:
            self.tryUpdateResultTitle(retdata)
            retdata['result']['metadata']['played_duration'] = duration + 20
        return retdata

        
class Backup:
    
    def __init__(self, config, dlog):
        self._sql = "insert into result_info (access_key, stream_url, stream_id, result, timestamp, catchDate) values (%s, %s, %s, %s, %s, CURRENT_DATE()) on duplicate key update id=LAST_INSERT_ID(id)"
        self._config = config
        db_config = self._config["database"]
        self._redis_map = {}
        self.dlog = dlog
        self._mdb = MysqlManager(host=db_config["host"],
                             port=db_config["port"],
                             user=db_config["user"],
                             passwd=db_config["passwd"],
                             dbname=db_config["db"])
        self._log_dir = self._config["log"]["dir"]
        self._result_filter = ResultFilter(self.dlog)
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
        filter_chinese = old_data.get("filter_chinese", 1)
        if 'response' in old_data['result']:
            old_data['result'] = old_data['result']['response']
            

        #从pem_file中清除pem_file
        old_data['pem_file'] = ''
            
        data = None
        if isCustom:
            data = self._result_filter.handleResult_custom(old_data)
        else:
            data = self._result_filter.handleResult(old_data)
        if data is None:
            return False

        access_key = data.get('access_key', 'unknow')
        stream_url = data.get('stream_url', 'unknow')
        stream_id = data.get('stream_id', 'unknow')

        title = self._result_filter.tryUpdateResultTitle(data)
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

        if redis_title != title and NORESULT != title:
            result = data.get('result', {})
            if result and filter_chinese:
                result = self.filter_chinese(stream_id, result)
            if self._mdb and result:                
                data['result'] = result  #之前涉及到的拷贝都是浅拷贝，这里的修改会影响到浅拷贝的变量history
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
        filter_chinese = old_data.get("filter_chinese", 1)
        if 'response' in old_data['result']:
            old_data['result'] = old_data['result']['response']


        #从pem_file中清除pem_file
        old_data['pem_file'] = ''
            
        data = None
        if isCustom:
            data = self._result_filter.delay_deal_custom(old_data)
        else:
            data = self._result_filter.delay_deal(old_data)
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
