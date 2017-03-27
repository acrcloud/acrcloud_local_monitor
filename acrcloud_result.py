#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys
import math
import json
import copy
import time
import Queue
import MySQLdb
import datetime
import threading
import traceback
import tools_str_sim
import tools_language
import multiprocessing
from dateutil.relativedelta import *

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
        self.init_logger()
        self.data_backup = Backup(self._config, self.dlog)
        self.dlog.logger.warn("Warn@Acrcloud_Result Init Success!")

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
            if not self._running:
                break
            try:
                maininfo = self._mainQueue.get(block = False)
                if maininfo and maininfo[0] == 'stop':
                    self.stop()
            except Queue.Empty:
                pass

            try:
                resinfo = self._resultQueue.get(block = False)
            except Queue.Empty:
                continue

            self.deal_result(resinfo)
            time.sleep(0.5)

    def stop(self):
        self._running = False
        self.dlog.logger.warn('Warn@Acrcloud_Result_Stop')

class ResultFilter:

    def __init__(self, dlog, log_dir):
        self._dlog = dlog
        self._log_dir = log_dir
        self._real_music = {}
        self._real_music_list_num = 3
        self._real_custom = {}
        self._real_custom_list_num = 3
        self._real_custom_valid_interval = 5*60
        self._delay_music = {}
        self._delay_music_last_result = {}
        self._delay_custom = {}
        self._delay_list_max_num = 30
        self._delay_list_threshold = 70#self._delay_list_max_num*3
        self._recovery_interval = 24*60*60 #只恢复24小时之内的备份数据

    def get_mutil_result_title(self, data, itype='music', isize = 1):
        ret_list = []
        index = 0
        json_res = data["result"]
        if json_res == NORESULT:
            return [NORESULT]
        try:
            if json_res['status']['code'] == 0:
                if itype == 'music':
                    if 'metadata' in json_res and 'music' in json_res['metadata']:
                        for item in json_res['metadata']['music']:
                            ret_list.append(item['title'])
                            index += 1
                            if index >= isize:
                                break
                    elif 'metainfos' in json_res:
                        for item in json_res['metainfos']:
                            ret_list.append(item['title'])
                            index += 1
                            if index >= isize:
                                break
                elif itype == 'custom':
                    if 'metadata' in json_res and 'custom_files' in json_res['metadata']:
                        for item in json_res['metadata']['custom_files']:
                            ret_list.append(item['title'])
                            index += 1
                            if index >= isize:
                                break
        except Exception as e:
            self._dlog.logger.error("Error@get_mutil_result_title", exc_info=True)
            self._dlog.logger.error("Error_Data: {0}".format(data))
        return ret_list if ret_list else [NORESULT]

    def get_mutil_result_acrid(self, data, itype='music', isize = 1):
        ret_list = []
        index = 0
        json_res = data["result"]
        if json_res == NORESULT:
            return [NORESULT]
        try:
            if json_res['status']['code'] == 0:
                if itype == 'music':
                    if 'metadata' in json_res and 'music' in json_res['metadata']:
                        for item in json_res['metadata']['music']:
                            ret_list.append(item['acrid'])
                            index += 1
                            if index >= isize:
                                break
                    elif 'metainfos' in json_res:
                        for item in json_res['metainfos']:
                            ret_list.append(item['acrid'])
                            index += 1
                            if index >= isize:
                                break
                elif itype == 'custom':
                    if 'metadata' in json_res and 'custom_files' in json_res['metadata']:
                        for item in json_res['metadata']['custom_files']:
                            ret_list.append(item['acrid'])
                            index += 1
                            if index >= isize:
                                break
        except Exception as e:
            self._dlog.logger.error("Error@get_mutil_result_acrid", exc_info=True)
            self._dlog.logger.error("Error_Data: {0}".format(json.dumps(result)))
        return ret_list if ret_list else [NORESULT]

    def swap_position(self, ret_title, ret_data, itype):
        json_res = ret_data["result"]
        meta_type = None
        music_list = []
        if itype == 'music':
            if 'metadata' in json_res:
                music_list = json_res['metadata']['music']
            elif 'metainfos' in json_res:
                music_list = json_res['metainfos']
        elif itype == 'custom':
            music_list = json_res['metadata']['custom_files']

        if music_list:
            ret_index = 0
            for index, item in enumerate(music_list):
                if itype == "music":
                    if item['title'] == ret_title:
                        ret_index = index
                        break
                else:
                    if item['acrid'] == ret_title:
                        ret_index = index
                        break
            if ret_index > 0:
                music_list[0], music_list[ret_index] = music_list[ret_index], music_list[0]

    def custom_result_append(self, ret_data, title, from_data, count):
        for item in from_data['result']['metadata']['custom_files']:
            if item["acrid"] == title:
                item["count"] = count
                ret_data['result']['metadata']['custom_files'].append(item)

    def get_play_offset(self, data, itype='music'):
        try:
            play_offset_ms = 0 #单位ms
            result = data['result']
            if result['status']['code'] == 1001:
                return 0
            if itype == 'music':
                play_offset_ms = result['metadata']['music'][0]['play_offset_ms']
            elif itype == 'custom':
                play_offset_ms = result['metadata']['custom_files'][0]['play_offset_ms']
        except Exception as e:
            self._dlog.logger.error("Error@Get_Play_Offset, error_data: {0}, {1}".format(itype, data), exc_info=True)
        return play_offset_ms/1000.0

    def get_db_play_offset(self, data, offset_type="begin", itype='music'):
        """
        itype : music or custom
        offset_type : begin or end offset
        """
        try:
            if offset_type not in ['begin', 'end']:
                self._dlog.logger.error("Error@Get_DB_Play_Offset.offset_type({0}) error".format(offset_type))
                return (None, self.get_play_offset(data, itype)) #if offset_type error, return play_offset_ms

            db_offset_key = "db_{0}_time_offset_ms".format(offset_type)
            sample_offset_key = "sample_{0}_time_offset_ms".format(offset_type)

            db_play_offset_ms = 0 #单位ms
            sample_play_offset_ms = 0
            result = data['result']
            if result['status']['code'] == 1001:
                return 0
            if itype == 'music':
                db_play_offset_ms = result['metadata']['music'][0][db_offset_key]
                sample_play_offset_ms = result['metadata']['music'][0][sample_offset_key]
            elif itype == 'custom':
                db_play_offset_ms = result['metadata']['custom_files'][0][db_offset_key]
                sample_play_offset_ms = result['metadata']['custom_files'][0][sample_offset_key]
        except Exception as e:
            self._dlog.logger.error("Error@Get_DB_Play_Offset, error_data: {0}, {1}, {2}".format(offset_type, itype, data), exc_info=True)
        return (int(sample_play_offset_ms)/1000.0, int(db_play_offset_ms)/1000.0)

    def get_duration(self, end_timestamp, start_timestamp):
        end = datetime.datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S')
        start = datetime.datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S')
        return (end - start).seconds

    def get_duration_accurate(self, end_data, start_data, itype='music'):
        monitor_len = end_data.get('monitor_seconds', 10)
        end_play_offset = self.get_play_offset(end_data, itype)
        start_play_offset = self.get_play_offset(start_data, itype)
        pre_seconds = max(20, monitor_len*2) #弥补前奏识别不到
        if int(start_play_offset) < pre_seconds:
            start_play_offset = 0
        else:
            start_play_offset = start_play_offset - (monitor_len/2)
        return int(round(end_play_offset - start_play_offset))

    def get_duration_accurate_use_db_offset(self, end_data, begin_data, isize, itype='music'):
        begin_timestamp = datetime.datetime.strptime(begin_data['timestamp'], "%Y-%m-%d %H:%M:%S")

        monitor_len = end_data.get('monitor_seconds', 10)

        end_sample_offset, end_db_offset = self.get_db_play_offset(end_data, 'end', itype)
        begin_sample_offset, begin_db_offset = self.get_db_play_offset(begin_data, 'begin', itype)

        accurate_begin_timestamp = (begin_timestamp + relativedelta(seconds=int(float(begin_sample_offset)))).strftime("%Y-%m-%d %H:%M:%S")

        db_len = int(round(end_db_offset - begin_db_offset))
        sample_len = int(round(end_sample_offset - begin_sample_offset + (isize-1)*monitor_len))

        mix_len = 0
        if int(begin_sample_offset) == 0 and int(begin_db_offset) == 0:
            mix_len = (isize-1)*monitor_len + end_sample_offset
        elif int(begin_sample_offset) == 0:
            if begin_db_offset <= monitor_len:
                mix_len = begin_db_offset + (isize-1)*monitor_len + end_sample_offset
            else:
                mix_len = (isize-1)*monitor_len + end_sample_offset - begin_sample_offset
        elif int(begin_db_offset) == 0:
            mix_len = (isize-1)*monitor_len + end_sample_offset - begin_sample_offset
        else:
            mix_len = (isize-1)*monitor_len + end_sample_offset - begin_sample_offset
        mix_len = int(math.ceil(mix_len)) + 1

        return sample_len, db_len, mix_len, accurate_begin_timestamp

    def judge_zero_item_contain_current_result(self, ret_sim_title, zero_data, itype="music"):
        """
        itype: music => title is track name
        itype: custom => title is acrid
        """
        try:
            is_contain = False
            if itype == "music":
                zero_title_list = self.get_mutil_result_title(zero_data, 'music', 5)
            elif itype == "custom":
                zero_title_list = self.get_mutil_result_acrid(zero_data, 'custom', 5)
            else:
                return is_contain

            for ztitle in zero_title_list:
                if ztitle == NORESULT:
                    break
                sim_zero_title = self.tryStrSub(ztitle)[0] if itype == "music" else ztitle
                #print "zero sim title: {0} -- {1}".format(ret_sim_title, sim_zero_title)
                if sim_zero_title == ret_sim_title:
                    is_contain = True
                    self.swap_position(ztitle, zero_data, itype)
                    break
        except Exception as e:
            self._dlog.logger.error("Error@judge_zero_item_contain_current_result", exc_info=True)
        return is_contain

    def judge_latter_item_contain_current_result(self, ret_sim_title, latter_data, itype="music"):
        """
        itype: music => title is track name
        itype: custom => title is acrid
        """
        try:
            is_contain = False
            latter_data_swaped = None
            if itype == "music":
                latter_title_list = self.get_mutil_result_title(latter_data, 'music', 5)
            elif itype == "custom":
                latter_title_list = self.get_mutil_result_acrid(latter_data, 'custom', 5)
            else:
                return is_contain, latter_data_swaped

            for ltitle in latter_title_list:
                if ltitle == NORESULT:
                    break
                sim_latter_title = self.tryStrSub(ltitle)[0] if itype == "music" else ltitle
                #print "latter sim title: {0} -- {1}".format(ret_sim_title, sim_latter_title)
                if sim_latter_title == ret_sim_title:
                    is_contain = True
                    latter_data_swaped = copy.deepcopy(latter_data)
                    self.swap_position(ltitle, latter_data_swaped, itype)
                    break
        except Exception as e:
            self._dlog.logger.error("Error@judge_latter_item_contain_current_result", exc_info=True)
        return is_contain, latter_data_swaped

    def real_check_title_custom(self, stream_id, title):
        now_timestamp = datetime.datetime.utcnow()
        if stream_id not in self._real_custom:
            self._real_custom[stream_id] = [[('','')], '']

        if len(self._real_custom[stream_id][0]) > self._real_custom_list_num:
            self._real_custom[stream_id][0] = self._real_custom[stream_id][0][-self._real_custom_list_num:]
            his_list_num = self._real_custom_list_num
        else:
            his_list_num = len(self._real_custom[stream_id][0])

        for i in range(his_list_num-1, -1, -1):
            if self._real_custom[stream_id][0][i][0] == title:
                his_timestamp = self._real_custom[stream_id][0][i][1]
                his_time_obj = datetime.datetime.strptime(his_timestamp, '%Y-%m-%d %H:%M:%S')
                if (now_timestamp - his_time_obj).seconds <= self._real_custom_valid_interval:
                    return True
            if title == NORESULT:
                break

        return False

    def checkResultSim(self, idx, curr_title, his_title, stream_id):
        if not curr_title or not his_title:
            return False
        sim, detail = tools_str_sim.str_sim(curr_title, his_title)
        if not sim and curr_title != NORESULT and his_title != NORESULT:
            self._dlog.logger.info("Sim@StreamID: {0}, CurrTitle: {1}, HisTitle: {2}({3}), Sim: {4}".format(str(stream_id), curr_title, his_title, str(idx), str(detail)))
        return sim

    def checkSame(self, curr_title, stream_id):
        self._real_music[stream_id] = self._real_music.get(stream_id, [[''], ''])
        if len(self._real_music[stream_id][0]) > self._real_music_list_num:
            self._real_music[stream_id][0] = self._real_music[stream_id][0][-self._real_music_list_num:]
            his_max = self._real_music_list_num
        else:
            his_max = len(self._real_music[stream_id][0])
        for i in range(his_max-1, -1, -1):
            if self.checkResultSim(i, curr_title, self._real_music[stream_id][0][i], stream_id):
                return True
            if curr_title == NORESULT:
                break
        return False

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

    def tryUpdateResultTitle(self, data, itype):
        if itype == 'custom':
            title = self.get_mutil_result_title(data, 'custom', 1)[0]
            return title
        title = self.get_mutil_result_title(data, 'music', 1)[0]
        stream_id = data.get("stream_id")
        new_title, try_status = self.tryStrSub(title)
        if try_status:
            self.updateResultTitle(data, new_title)
            self._dlog.logger.info("StreamID: {0}, Update Title: [{1}] >>> [{2}]".format(stream_id, title, new_title))
            return new_title
        return title

    def deal_real_history(self, data):
        is_new = False
        result = None
        curr_title = self.get_mutil_result_title(data, 'music', 1)[0]
        stream_id = data.get("stream_id")
        if not stream_id:
            return result, is_new
        if curr_title == NORESULT:
            if not self.checkSame(curr_title, stream_id):
                self._real_music[stream_id][0].append(curr_title)
                self._real_music[stream_id][1] = data
                result = data
                is_new = True
            else:
                result = None
                is_new = False
        else:
            if self.checkSame(curr_title, stream_id):
                result = self._real_music[stream_id][1]
                is_new = False
            else:
                self._real_music[stream_id][0].append(curr_title)
                self._real_music[stream_id][1] = data
                result = data
                is_new = True

        return result, is_new

    def deal_delay_history(self, data):
        stream_id = data.get("stream_id")
        timestamp = data.get("timestamp")
        raw_title = self.get_mutil_result_title(data, 'music', 1)[0]
        sim_title = self.tryStrSub(raw_title)
        if stream_id not in self._delay_music:
            self._delay_music[stream_id] = [(raw_title, sim_title[0], timestamp, data)]
        else:
            self._delay_music[stream_id].append((raw_title, sim_title[0], timestamp, data))

        if len(self._delay_music[stream_id]) > self._delay_list_max_num :
            return self.runDelayX(stream_id)
        else:
            return None

    def compute_played_duration(self, history_data, start_index, end_index, judge_zero_or_latter=True, itype="music"):
        retdata = history_data[start_index][-1]

        if itype == "music":
            ret_title = self.get_mutil_result_title(retdata, 'music', 1)[0]
            ret_sim_title = history_data[start_index][1]
        elif itype == "custom":
            ret_title = self.get_mutil_result_acrid(retdata, 'custom', 1)[0]
            ret_sim_title = ret_title

        if judge_zero_or_latter and start_index == 1:
            if self.judge_zero_item_contain_current_result(ret_sim_title, history_data[0][-1], itype):
                start_index = 0

        is_contain = False
        latter_data_swaped = None
        if judge_zero_or_latter and (end_index + 1 <= len(history_data) - 1):
            is_contain, latter_data_swaped = self.judge_latter_item_contain_current_result(ret_sim_title, history_data[end_index+1][-1], itype)

        if itype == "music":
            start_timestamp = history_data[start_index][2]
            end_timestamp = history_data[end_index][2]
            start_data = history_data[start_index][3]
            end_data = history_data[end_index][3]
        else:
            start_timestamp = history_data[start_index][1]
            end_timestamp = history_data[end_index][1]
            start_data = history_data[start_index][2]
            end_data = history_data[end_index][2]

        duration = self.get_duration(end_timestamp, start_timestamp)
        duration_accurate = self.get_duration_accurate(end_data, start_data, itype)
        isize = end_index - start_index + 1
        if is_contain:
            end_data = latter_data_swaped
            isize += 1

        sample_duraion, db_duration, mix_duration, accurate_timestamp_utc = self.get_duration_accurate_use_db_offset(end_data, start_data, isize, itype)

        ret_dict = {
            "duration" : duration,
            "duration_accurate" : duration_accurate,
            "sample_duration" : sample_duraion,
            "db_duration" : db_duration,
            "mix_duration" : mix_duration,
            "accurate_timestamp_utc" : accurate_timestamp_utc,
        }
        return ret_dict

    def runDelayX(self, stream_id):
        history_data = self._delay_music[stream_id]

        if len(history_data) >= self._delay_list_threshold:
            self._dlog.logger.error("delay_music[{0}] list num({1}) over threshold {2}".format(stream_id, len(history_data), self._delay_list_threshold))
            self._dlog.logger.error("delay_music[{0}] data: \n{1}".format(stream_id, '\n'.join([str(item[:-1]) for item in history_data])))
            history_data = history_data[-self._delay_list_max_num:]

        sim_title_set = set()
        sim_title_count = {}
        for index, item in enumerate(history_data):
            if index == 0:
                continue
            if item[1] not in sim_title_set:
                sim_title_count[item[1]] = [1, [index, ]]
                sim_title_set.add(item[1])
            else:
                sim_title_count[item[1]][0] += 1
                sim_title_count[item[1]][1].append(index)
        sim_title_count_single_index = [sim_title_count[key][1][0] for key in sim_title_count if sim_title_count[key][0] == 1]

        if len(history_data)-1 in sim_title_count_single_index:
            sim_title_count_single_index.remove(len(history_data)-1)

        deal_num = 3 #将noresult和单个count的index添加到del_index

        del_index = set()
        order_key_list = []
        order_set = set()
        for index, item in enumerate(history_data):
            if index == 0:
                continue
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
        duration_dict = None
        another_del_index = set()

        if len(order_key_list) == 3:
            first_item = sim_title_count[order_key_list[0]]
            second_item = sim_title_count[order_key_list[1]]
            third_item = sim_title_count[order_key_list[2]]
            xflag = 0 #0第一个和后两个无交集，1第一个只和第二个有交集，2第一个和后两个都有交集
            if first_item[1][-1] < second_item[1][0]:
                xflag = 0 if first_item[1][-1] < third_item[1][0] else 2
            else:
                if first_item[1][-1] < third_item[1][0]:
                    xflag = 1 if second_item[1][-1] < third_item[1][0] else 2
                else:
                    xflag = 2
            if xflag == 0:
                start_index = first_item[1][0]
                end_index = first_item[1][-1]
                retdata = history_data[start_index][-1]
                duration_dict =  self.compute_played_duration(history_data, start_index, end_index)

                another_del_index = set(first_item[1])
            elif xflag == 1:
                lucky_item = first_item if first_item[0] >= second_item[0] else second_item
                retdata = history_data[lucky_item[1][0]][-1]
                start_index = first_item[1][0] if first_item[1][0] < second_item[1][0] else second_item[1][0]
                end_index = first_item[1][-1] if first_item[1][-1] > second_item[1][-1] else second_item[1][-1]

                duration_dict =  self.compute_played_duration(history_data, start_index, end_index, False)

                another_del_index = set(first_item[1])
                another_del_index = another_del_index.union(set(second_item[1]))
            elif xflag == 2:
                lucky_item = first_item if first_item[0] >= second_item[0] else second_item
                lucky_item = lucky_item if lucky_item[0] >= third_item[0] else third_item
                retdata = history_data[lucky_item[1][0]][-1]
                start_index = min(first_item[1][0], second_item[1][0], third_item[1][0])
                end_index = max(first_item[1][-1], second_item[1][-1], third_item[1][-1])

                duration_dict =  self.compute_played_duration(history_data, start_index, end_index, False)

                another_del_index = set(first_item[1])
                another_del_index = another_del_index.union(set(second_item[1]))
                another_del_index = another_del_index.union(set(third_item[1]))
        elif len(order_key_list) == 2:
            first_item = sim_title_count[order_key_list[0]]
            second_item = sim_title_count[order_key_list[1]]
            if first_item[1][-1] < second_item[1][0]:
                start_index = first_item[1][0]
                end_index = first_item[1][-1]
                retdata = history_data[start_index][-1]

                duration_dict =  self.compute_played_duration(history_data, start_index, end_index)

                another_del_index = set(first_item[1])
            else:
                lucky_item = first_item if first_item[0] >= second_item[0] else second_item
                retdata = history_data[lucky_item[1][0]][-1]
                start_index = min(first_item[1][0], second_item[1][0])
                end_index = max(first_item[1][-1], second_item[1][-1])

                duration_dict =  self.compute_played_duration(history_data, start_index, end_index, False)

                another_del_index = set(first_item[1])
                another_del_index = another_del_index.union(set(second_item[1]))
        elif len(order_key_list) == 1:
            start_index = sim_title_count[order_key_list[0]][1][0]
            end_index = sim_title_count[order_key_list[0]][1][-1]
            retdata = history_data[start_index][-1]

            duration_dict =  self.compute_played_duration(history_data, start_index, end_index)

            another_del_index = set(sim_title_count[order_key_list[0]][1])

        if another_del_index and max(another_del_index) < len(history_data)-1:
            del_index = del_index.union(another_del_index)
        else:
            retdata = None

        del_index.add(0)
        max_del_index = max(del_index)
        del_index.remove(max_del_index)

        after_del_history_data = []
        for index in range(len(history_data)):
            if index not in del_index:
                after_del_history_data.append(history_data[index])

        self._delay_music[stream_id] = after_del_history_data

        #compare now retdata title and last_result title
        #if equal, and time interval in 10min, then return None
        if retdata:
            ret_timestamp = retdata.get("timestamp")
            ret_title = self.get_mutil_result_title(retdata, 'music', 1)[0]
            if self._delay_music_last_result.has_key(stream_id):
                if ret_title == self._delay_music_last_result[stream_id][0]:
                    ret_time_obj = datetime.datetime.strptime(ret_timestamp, '%Y-%m-%d %H:%M:%S')
                    last_time_obj = datetime.datetime.strptime(self._delay_music_last_result[stream_id][1], '%Y-%m-%d %H:%M:%S')
                    if (ret_time_obj - last_time_obj).seconds < 10*60:
                        retdata = None
                else:
                    self._delay_music_last_result[stream_id] = (ret_title, ret_timestamp)
            else:
                self._delay_music_last_result[stream_id] = (ret_title, ret_timestamp)

        if retdata:
            duration = duration_dict["duration"]
            duration_accurate = duration_dict["duration_accurate"]
            sample_duration = duration_dict["sample_duration"]
            db_duration = duration_dict["db_duration"]
            mix_duration = duration_dict["mix_duration"]
            accurate_timestamp_utc = duration_dict["accurate_timestamp_utc"]

            retdata['result']['metadata']['played_duration'] = mix_duration
            retdata['result']['metadata']['timestamp_utc'] = accurate_timestamp_utc

        return retdata

    def deal_real_custom(self, data):

        is_new = False
        result = None
        #curr_title = self.get_mutil_result_title(data, 'custom')[0]
        curr_title = self.get_mutil_result_acrid(data, 'custom')[0]

        stream_id = data.get("stream_id")
        timestamp = data.get("timestamp")
        if not stream_id:
            return result, is_new
        if curr_title == NORESULT:
            if not self.real_check_title_custom(stream_id, curr_title):
                self._real_custom[stream_id][0].append((curr_title, timestamp))
                self._real_custom[stream_id][1] = data
                result = data
                is_new = True
            else:
                result = None
                is_new = False
        else:
            if self.real_check_title_custom(stream_id, curr_title):
                result = self._real_custom[stream_id][1]
                is_new = False
            else:
                self._real_custom[stream_id][0].append((curr_title, timestamp))
                self._real_custom[stream_id][1] = data
                result = data
                is_new = True
        return result, is_new

    def deal_delay_custom(self, data):
        try:
            ret_result = None
            stream_id = data.get("stream_id")
            timestamp = data.get("timestamp")
            #title_list = self.get_mutil_result_title(data, 'custom')
            title_list = self.get_mutil_result_acrid(data, 'custom', 1)
            if stream_id not in self._delay_custom:
                self._delay_custom[stream_id] = [(title_list, timestamp, data)]
            else:
                self._delay_custom[stream_id].append((title_list, timestamp, data))

            ret_result = self.runDelayX_custom(stream_id)
        except Exception as e:
            self._dlog.logger.error("Error@deal_delay_custom", exc_info=True)
        return ret_result

    def runDelayX_custom(self, stream_id):
        history_data = self._delay_custom[stream_id]

        if len(history_data) >= self._delay_list_threshold:
            self._dlog.logger.error("delay_custom[{0}] list num({1}) over threshold {2}".format(stream_id, len(history_data), self._delay_list_threshold))
            self._dlog.logger.error("delay_custom[{0}] data: \n{1}".format(stream_id, '\n'.join([str(item[:-1]) for item in history_data])))
            history_data = history_data[-self._delay_list_threshold:]
            history_data.append(([NORESULT], "", ""))
            history_data.append(([NORESULT], "", ""))
            history_data.append(([NORESULT], "", ""))
            history_data.append(([NORESULT], "", ""))

        ########## Get Break Index ##########
        deal_title_map = {} #key:title, value:{'count':0, 'index_list':[]}
        break_index = 0

        for index, item in enumerate(history_data[1:]):
            index += 1
            title_list, timestamp, data = item
            if index!=0:
                flag_first = True
                flag_second = True
                for title in title_list:
                    if title in deal_title_map:
                        flag_first = False
                if flag_first:
                    for i in range(1,5):
                        if index + i < len(history_data):
                            next_title_list, next_timestamp, next_data = history_data[index + i]
                            for title in next_title_list:
                                if title in deal_title_map:
                                    flag_second = False
                        else:
                            flag_second = False
                if flag_first and flag_second and deal_title_map:
                    break_index = index  #[0, index), index是需要处理的最后一个索引的下一个
                    break

            for title in title_list:
                if title == NORESULT:
                    continue
                if title not in deal_title_map:
                    deal_title_map[title] ={'count':0, 'index_list':[]}
                deal_title_map[title]['count'] += 1
                deal_title_map[title]['index_list'].append(index)

        ########### New Deal Custom Result Add Count ###########
        ret_data = None
        duration_dict = {}
        duration = 0
        if break_index > 0 and deal_title_map:
            tmp_count_map = {}
            sorted_title_list = sorted(deal_title_map.items(), key = lambda x:x[1]['count'], reverse = True)
            for sitem in sorted_title_list:
                sitem_title, sitem_map = sitem
                sitem_count = sitem_map["count"]
                sitem_min_index = min(sitem_map["index_list"])
                if sitem_count not in tmp_count_map:
                    tmp_count_map[sitem_count] = []
                tmp_count_map[sitem_count].append((sitem_title, sitem_min_index))
            first_item_flag = True
            for scount in sorted(tmp_count_map.keys(), reverse=True):
                count_list = sorted(tmp_count_map[scount], key = lambda x:x[1])
                for ditem in count_list:
                    dtitle, dindex = ditem
                    from_data = history_data[dindex][2]
                    if first_item_flag:
                        first_item_flag = False
                        ret_data = copy.deepcopy(from_data)
                        ret_data["result"]["metadata"]["custom_files"] = []
                    self.custom_result_append(ret_data, dtitle, from_data, scount)

            index_range = set()
            for title in deal_title_map:
                index_range |= set(deal_title_map[title]['index_list'])
            min_index = min(index_range)
            max_index = max(index_range)
            duration_dict = self.compute_played_duration(history_data, min_index, max_index, True, "custom")

        if ret_data:
            duration = duration_dict["duration"]
            duration_accurate = duration_dict["duration_accurate"]
            sample_duration = duration_dict["sample_duration"]
            db_duration = duration_dict["db_duration"]
            mix_duration = duration_dict["mix_duration"]
            accurate_timestamp_utc = duration_dict["accurate_timestamp_utc"]
            ret_data['result']['metadata']['played_duration'] = mix_duration
            ret_data['result']['metadata']['timestamp_utc'] = accurate_timestamp_utc

        ########### cut history_data #############
        if break_index>=0:
            cut_index = break_index
            for i, item in enumerate(history_data[break_index:]):
                if item[0][0] == NORESULT:
                    cut_index = break_index + i + 1
                else:
                    break
            cut_index = cut_index - 1 if cut_index >= 1 else cut_index
            history_data = history_data[cut_index:]
            self._delay_custom[stream_id] = history_data
        return ret_data


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
        filter_chinese = old_data.get("filter_chinese", 1)
        if 'response' in old_data['result']:
            old_data['result'] = old_data['result']['response']

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
