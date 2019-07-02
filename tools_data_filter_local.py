#!/usr/bin/env python
#-*- coding:utf-8 -*-

import os
import sys
import json
import copy
import math
import random
import requests
import datetime
import traceback
import tools_str_sim
from dateutil.relativedelta import *

reload(sys)
sys.setdefaultencoding("utf8")

NORESULT = "noResult"

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
        self._delay_music_interval_threshold = 2*60
        self._delay_custom = {}
        self._delay_custom_played_duration_min = 1
        self._delay_music_played_duration_min = 5
        self._delay_list_max_num = 35
        self._delay_list_threshold = 70

    def is_noresult(self, instr):
        if instr in ["noResult", "noresult", NORESULT]:
            return True
        else:
            return False

    def clean_buf(self, stream_id):
        try:
            if stream_id in self._real_music:
                del self._real_music[stream_id]
            if stream_id in self._real_custom:
                del self._real_custom[stream_id]
            if stream_id in self._delay_music:
                del self._delay_music[stream_id]
            if stream_id in self._delay_custom:
                del self._delay_custom[stream_id]
        except Exception as e:
            self._dlog.logger.error("Error@clean_buf", exc_info=True)

    def show_log(self, stream_id, data_list, index_list, itype="Before"):
        try:
            for index, item in enumerate(data_list):
                tmp = [str(index)] + [item[i] for i in index_list]
                print ", ".join(tmp)
        except Exception as e:
            self._dlog.logger.error("Error@show_log", exc_info=True)

    def get_mutil_result_title(self, data, itype='music', isize = 1):
        ret_list = []
        index = 0
        json_res = data["result"]
        if self.is_noresult(json_res):
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
        if self.is_noresult(json_res):
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
                            score = item.get('score', 80)
                            if score >= 100 or index == 0:
                                ret_list.append(item['acrid'])
                            else:
                                break
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

    def custom_result_append(self, ret_data, title, from_data, count, tmp_deal_title_map):
        try:
            ret_title_set = set()
            for item in ret_data['result']['metadata']['custom_files']:
                ret_title_set.add(item['acrid'])

            for item in from_data['result']['metadata']['custom_files']:
                acrid = item['acrid']
                if acrid == title and acrid not in ret_title_set:
                    item['count'] = count
                    ret_data['result']['metadata']['custom_files'].append(item)
                    ret_title_set.add(acrid)
        except Exception as e:
            self._dlog.logger.error("Error@custom_result_append, ret_date:{0}, title:{1}, from_data:{2}".format(ret_data, title, from_data), exc_info=True)


    def get_play_offset(self, data, itype='music'):
        try:
            play_offset_ms = 0 #ms
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

    def get_db_play_offset(self, begin_data, end_data, offset_type="begin", itype='music'):
        """
        itype : music or custom
        offset_type : begin or end offset
        """
        try:
            data = None
            if offset_type == "begin":
                data = begin_data
            else:
                data = end_data

            if offset_type not in ['begin', 'end']:
                self._dlog.logger.error("Error@Get_DB_Play_Offset.offset_type({0}) error".format(offset_type))
                return (None, self.get_play_offset(data, itype)) #if offset_type error, return play_offset_ms

            db_offset_key = "db_{0}_time_offset_ms".format(offset_type)
            sample_offset_key = "sample_{0}_time_offset_ms".format(offset_type)

            db_play_offset_ms = 0
            sample_play_offset_ms = 0
            result = data['result']
            if result['status']['code'] == 1001:
                return 0
            if itype == 'music':
                db_play_offset_ms = result['metadata']['music'][0][db_offset_key]
                sample_play_offset_ms = result['metadata']['music'][0][sample_offset_key]
            elif itype == 'custom':
                if offset_type == "begin":
                    db_play_offset_ms = result['metadata']['custom_files'][0][db_offset_key]
                    sample_play_offset_ms = result['metadata']['custom_files'][0][sample_offset_key]
                else:
                    begin_acrid = None
                    end_index = 0
                    try:
                        begin_acrid = begin_data["result"]["metadata"]["custom_files"][0]["acrid"]
                    except Exception as e:
                        traceback.print_exc()
                    for index, item in enumerate(end_data["result"]["metadata"]["custom_files"]):
                        if begin_acrid == item["acrid"]:
                            end_index = index
                    db_play_offset_ms = result['metadata']['custom_files'][end_index][db_offset_key]
                    sample_play_offset_ms = result['metadata']['custom_files'][end_index][sample_offset_key]
        except Exception as e:
            self._dlog.logger.error("Error@Get_DB_Play_Offset, error_data: {0}, {1}, {2}".format(offset_type, itype, data), exc_info=True)
        return (int(sample_play_offset_ms)/1000.0, int(db_play_offset_ms)/1000.0)

    def get_duration(self, end_timestamp, start_timestamp):
        end = datetime.datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S')
        start = datetime.datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S')
        return (end - start).total_seconds()

    def get_duration_accurate(self, end_data, start_data, itype='music'):
        monitor_len = end_data.get('monitor_seconds', 10)
        end_play_offset = self.get_play_offset(end_data, itype)
        start_play_offset = self.get_play_offset(start_data, itype)
        pre_seconds = max(20, monitor_len*2)
        if int(start_play_offset) < pre_seconds:
            start_play_offset = 0
        else:
            start_play_offset = start_play_offset - (monitor_len/2)
        return int(round(end_play_offset - start_play_offset))

    def get_duration_accurate_use_db_offset(self, end_data, begin_data, isize, itype='music'):
        begin_timestamp = datetime.datetime.strptime(begin_data['timestamp'], "%Y-%m-%d %H:%M:%S")

        monitor_len = end_data.get('monitor_seconds', 10)

        begin_sample_offset, begin_db_offset = self.get_db_play_offset(begin_data, end_data, 'begin', itype)
        end_sample_offset, end_db_offset = self.get_db_play_offset(begin_data, end_data, 'end', itype)

        accurate_begin_timestamp = (begin_timestamp + relativedelta(seconds=int(float(begin_sample_offset)))).strftime("%Y-%m-%d %H:%M:%S")

        db_len = abs(int(round(end_db_offset - begin_db_offset)))
        sample_len = abs(int(round(end_sample_offset - begin_sample_offset + (isize-1)*monitor_len)))

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
        mix_len = abs(int(math.ceil(mix_len)))

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
                if self.is_noresult(ztitle):
                    break
                sim_zero_title = self.tryStrSub(ztitle)[0] if itype == "music" else ztitle
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
                if self.is_noresult(ltitle):
                    break
                sim_latter_title = self.tryStrSub(ltitle)[0] if itype == "music" else ltitle
                if sim_latter_title == ret_sim_title:
                    is_contain = True
                    latter_data_swaped = copy.deepcopy(latter_data)
                    self.swap_position(ltitle, latter_data_swaped, itype)
                    break
        except Exception as e:
            self._dlog.logger.error("Error@judge_latter_item_contain_current_result", exc_info=True)
        return is_contain, latter_data_swaped

    def real_check_title_custom(self, stream_id, title, timestamp_obj):
        now_timestamp = timestamp_obj #datetime.datetime.utcnow()
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
                if (now_timestamp - his_time_obj).total_seconds() <= self._real_custom_valid_interval:
                    return True
            if self.is_noresult(title):
                break

        return False

    def checkResultSim(self, idx, curr_title, his_title, stream_id):
        if not curr_title or not his_title:
            return False
        sim, detail = tools_str_sim.str_sim(curr_title, his_title)
        if not sim and not self.is_noresult(curr_title) and not self.is_noresult(his_title):
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
            if self.is_noresult(curr_title):
                break
        return False

    def updateResultTitle(self, data, new_title):
        if self.is_noresult(new_title):
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
        if self.is_noresult(try_str):
            return try_str, False
        sub_str = tools_str_sim.str_sub(try_str)
        if len(sub_str) > 0 and len(try_str) >= len(sub_str):
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
        if self.is_noresult(curr_title):
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

    def deal_delay_music1(self, data):
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

    def get_data_duration_ms(self, data):
        try:
            duration_ms = -1
            json_res = data["result"]
            if json_res['status']['code'] == 0:
                if 'metadata' in json_res and 'music' in json_res['metadata']:
                    if len(json_res['metadata']['music']) > 0:
                        duration_ms = json_res["metadata"]["music"][0]["duration_ms"]
        except Exception as e:
            self._dlog.logger.error("Error@get_data_duration_ms", exc_info=True)
        return (duration_ms/1000.0) if duration_ms != -1 else duration_ms

    def get_time_diff(self, start_timestamp, end_timestamp, tformat="%Y-%m-%d %H:%M:%S"):
        try:
            diff_sec = 0
            start_obj = datetime.datetime.strptime(start_timestamp, tformat)
            end_obj = datetime.datetime.strptime(end_timestamp, tformat)
            diff_sec = int((end_obj - start_obj).total_seconds())
        except Exception as e:
            self._dlog.logger.error("Error@get_diff_seconds", exc_info=True)
        return diff_sec

    def runDelayX(self, stream_id):
        history_data = self._delay_music[stream_id]

        if len(history_data) >= self._delay_list_threshold:
            self._dlog.logger.error("delay_music[{0}] list num({1}) over threshold {2}".format(stream_id, len(history_data), self._delay_list_threshold))
            self._dlog.logger.error("delay_music[{0}] data: \n{1}".format(stream_id, '\n'.join([str(item[:-1]) for item in history_data[:5]])))
            history_data = history_data[-self._delay_list_max_num:]

        sim_title_set = set()
        sim_title_count = {}
        for index, item in enumerate(history_data):
            if index == 0:
                continue
            if self.is_noresult(item[1]):
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

        deal_num = 3

        del_index = set()
        order_key_list = []
        order_set = set()
        for index, item in enumerate(history_data):
            if index == 0:
                continue
            if self.is_noresult(item[1]):
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

        judge_interval_map = {}  #key:title, value:[[1], [2,3], [4,5]...]
        tmp_order_key_list = copy.deepcopy(order_key_list)
        for tmp_key in tmp_order_key_list:
            judge_interval_map[tmp_key] = []
            index_list = sim_title_count[tmp_key][1]
            pre_timestamp = ""
            tmp_list = []
            for i, tmp_index in enumerate(index_list):
                if i == 0:
                    pre_timestamp = history_data[tmp_index][2]
                else:
                    now_timestamp = history_data[tmp_index][2]
                    diff_seconds = self.get_time_diff(pre_timestamp, now_timestamp)
                    if diff_seconds > self._delay_music_interval_threshold:
                        if tmp_list:
                            judge_interval_map[tmp_key].append(tmp_list)
                            tmp_list = []
                    pre_timestamp = history_data[tmp_index][2]
                tmp_list.append(tmp_index)
            if tmp_list:
                judge_interval_map[tmp_key].append(tmp_list)

            if len(judge_interval_map[tmp_key]) > 1:
                judge_flag = False
                for tmp_l in judge_interval_map[tmp_key]:
                    if len(tmp_l) > 1:
                        sim_title_count[tmp_key][1] = tmp_l
                        sim_title_count[tmp_key][0] = len(tmp_l)
                        judge_flag = True
                        break
                    else:
                        if tmp_l:
                            del_index |= set(tmp_l)
                if not judge_flag:
                    sim_title_count[tmp_key][0] = 0
                    sim_title_count[tmp_key][1] = []
                    order_key_list.remove(tmp_key)

        retdata = None
        duration_dict = None
        another_del_index = set()

        if len(order_key_list) == 3:
            first_item = sim_title_count[order_key_list[0]]
            second_item = sim_title_count[order_key_list[1]]
            third_item = sim_title_count[order_key_list[2]]
            xflag = 0
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


        #del_index 首先添加了noresult， another_del_index是存放当前返回track的index，如果满足条件，则合并
        if another_del_index and max(another_del_index) < len(history_data)-1:
            del_index = del_index.union(another_del_index)
        else:
            retdata = None

        del_index.add(0)
        #max_del_index = max(del_index)
        #del_index.remove(max_del_index)
        remove_del_index = -1
        del_index_tmp_list = sorted(list(del_index))
        for i, del_i in enumerate(del_index_tmp_list):
            if i != del_i:
                remove_del_index = del_index_tmp_list[i-1]
                break
        if remove_del_index == -1:
            remove_del_index = del_index_tmp_list[-1]
        if remove_del_index != -1:
            del_index.remove(remove_del_index)

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
                    if (ret_time_obj - last_time_obj).total_seconds() < 5*60:
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

            ret_duration = mix_duration
            duration_s = self.get_data_duration_ms(retdata)
            if duration_s != -1:
                diff_ret_duration = duration_s - ret_duration
                if diff_ret_duration < 0 and abs(diff_ret_duration) >= 60:
                    ret_duration = duration_accurate
                    self._dlog.logger.warn("Warn@stream_id:{0}, mix_duration({1}) > duration_s({2})+60, replace to duration_accurate({3})".format(stream_id, mix_duration, duration_s, duration_accurate))
            retdata['result']['metadata']['played_duration'] = abs(ret_duration)
            retdata['result']['metadata']['timestamp_utc'] = accurate_timestamp_utc
            retdata['result']['metadata']['timestamp_local'] = self.utc2local(accurate_timestamp_utc)

            if ret_duration == 0:
                retdata = None
        return retdata

    def utc2local(self, utc_str):
        try:
            local_str = ""
            utc = datetime.datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S")
            epoch = time.mktime(utc.timetuple())
            offset = datetime.datetime.fromtimestamp (epoch) - datetime.datetime.utcfromtimestamp (epoch)
            local = utc + offset
            local_str = local.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            self._dlog.logger.error("Error@utc2local, utc:{0}".format(utc_str), exc_info=True)
        return local_str

    def deal_real_custom(self, data):
        is_new = False
        result = None
        curr_title = self.get_mutil_result_acrid(data, 'custom')[0]

        stream_id = data.get("stream_id")
        timestamp = data.get("timestamp")
        timestamp_obj = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        if not stream_id:
            return result, is_new
        if self.is_noresult(curr_title):
            if not self.real_check_title_custom(stream_id, curr_title, timestamp_obj):
                self._real_custom[stream_id][0].append((curr_title, timestamp))
                self._real_custom[stream_id][1] = data
                result = data
                is_new = True
            else:
                result = None
                is_new = False
        else:
            if self.real_check_title_custom(stream_id, curr_title, timestamp_obj):
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
            title_list = self.get_mutil_result_acrid(data, 'custom', 5)
            if stream_id not in self._delay_custom:
                self._delay_custom[stream_id] = [(title_list, timestamp, data)]
            else:
                self._delay_custom[stream_id].append((title_list, timestamp, data))

            ret_result = self.runDelayX_custom(stream_id)
        except Exception as e:
            self._dlog.logger.error("Error@deal_delay_custom", exc_info=True)
        return ret_result

    def remove_next_result_from_now_result_list(self, history_data, ret_data, max_index):
        #Just for custom delay filter
        try:
            if ret_data and len(history_data) >= max_index+2:
                acrid_list, timestamp, next_data = history_data[max_index + 1]
                if next_data:
                    #update max size acrid_list to 20
                    next_acrid_list = self.get_mutil_result_acrid(next_data, 'custom', 20)
                    next_acrid_set = set(next_acrid_list)
                    new_ret_custom_files = []
                    for index, item in enumerate(ret_data["result"]["metadata"]["custom_files"]):
                        if index == 0 or (item["acrid"] not in next_acrid_set):
                            new_ret_custom_files.append(item)
                    ret_data["result"]["metadata"]["custom_files"] = new_ret_custom_files
        except Exception as e:
            self._dlog.logger.error("Error@remove_next_result_from_now_result_list", exc_info=True)

    def get_custom_duration_by_title(self, title, ret_data):
        try:
            duration = 0
            db_end_offset = 0
            for index, item in enumerate(ret_data["result"]["metadata"]["custom_files"]):
                if title == item["acrid"]:
                    duration_ms = int(item["duration_ms"])
                    db_end_offset_ms = int(item["db_end_time_offset_ms"])
                    if duration_ms >= 0:
                        duration = int(duration_ms/1000)
                    if db_end_offset_ms:
                        db_end_offset = int(db_end_offset_ms/1000)
        except Exception as e:
            self._dlog.logger.error("Error@get_custom_duration_by_title, error_data:{0}".format(ret_data), exc_info=True)
        return duration, db_end_offset

    def get_music_duration_by_title(self, title, ret_data):
        try:
            duration = 0
            db_end_offset = 0
            if "metadata" in ret_data["result"] and "music" in ret_data["result"]["metadata"]:
                for index, item in enumerate(ret_data["result"]["metadata"]["music"]):
                    if title == item["title"]:
                        duration_ms = int(item["duration_ms"])
                        db_end_offset_ms = int(item["db_end_time_offset_ms"])
                        if duration_ms >= 0:
                            duration = int(duration_ms/1000)
                        if db_end_offset_ms:
                            db_end_offset = int(db_end_offset_ms/1000)
        except Exception as e:
            self._dlog.logger.error("Error@get_custom_duration_by_title, error_data:{0}".format(ret_data), exc_info=True)
        return duration, db_end_offset

    def delay_dynamic_judge_size(self, deal_title_map, history_data, itype):
        try:
            judge_size = 5
            if itype == "custom":
                title = sorted(deal_title_map.items(), key=lambda x:x[1]["score"], reverse=True)[0][0]
            else:
                title = deal_title_map.keys()[0]
                if title and title.lower().startswith("first of the year"):
                    return 1

            index = deal_title_map[title]["index_list"][-1]
            if itype == "custom":
                ret_data = history_data[index][2]
            else:
                ret_data = history_data[index][3]

            monitor_len = ret_data.get("monitor_seconds", 10)

            if itype == "custom":
                duration, db_end_offset = self.get_custom_duration_by_title(title, ret_data)
            else:
                duration, db_end_offset = self.get_music_duration_by_title(title, ret_data)

            if db_end_offset > 0  and db_end_offset < duration:
                judge_size = abs(int(math.ceil(db_end_offset*1.0/monitor_len))) + 1
            if judge_size > 10:
                judge_size = 10
            if judge_size <= 3:
                judge_size = 3
                if itype == "custom":
                    judge_size = 1
        except Exception as e:
            self._dlog.logger.error("Error@delay_dynamic_judge_size", exc_info=True)
        return judge_size+1

    def get_score_duration_ms(self, data):
        try:
            score = 0
            if "metadata" in data['result']:
                if "custom_files" in data['result']['metadata']:
                    first_item = data['result']['metadata']["custom_files"][0]
                    score = first_item.get("score",0)
                    duration_sec = int(int(first_item.get("duration_ms", 0))/1000)
        except Exception as e:
            self._dlog.logger.error("Error@get_score_duration_ms", exc_info=True)
        return score, duration_sec

    def runDelayX_custom(self, stream_id):
        history_data = self._delay_custom[stream_id]

        if len(history_data) >= self._delay_list_threshold:
            self._dlog.logger.error("delay_custom[{0}] list num({1}) over threshold {2}".format(stream_id, len(history_data), self._delay_list_threshold))
            self._dlog.logger.error("delay_custom[{0}] data: \n{1}".format(stream_id, '\n'.join(["{0}: {1}".format(i, str(item[:-1])) for i,item in enumerate(history_data[:5])])))

            history_data = history_data[-(self._delay_list_threshold-1):]

            history_data_len = len(history_data)
            for ii in range((history_data_len-1), 0, -1):
                if self.is_noresult(history_data[-ii][0][0]):
                    continue
                else:
                    history_data = history_data[-(ii+1):]
                    break

        ########## Get Break Index ##########
        deal_title_map = {} #key:title, value:{'count':0, 'index_list':[]}
        break_index = 0

        for index, item in enumerate(history_data[1:]):
            index += 1
            title_list, timestamp, data = item
            if index!=1:
                flag_first = True
                flag_second = False
                for title in title_list[:1]:
                    if title in deal_title_map:
                        flag_first = False
                if flag_first:
                    tmp_all_len = len(history_data)
                    tmp_count = 0
                    tmp_first_break_index = -1
                    tmp_judge_size = self.delay_dynamic_judge_size(deal_title_map, history_data, "custom")
                    for i in range(index, tmp_all_len):
                        next_title_list, next_timestamp, next_data = history_data[i]
                        tmp_list_flag = False
                        for title in next_title_list[:5]:
                            if title in deal_title_map:
                                tmp_list_flag = True
                                tmp_count = 0
                                tmp_first_break_index = -1
                        if tmp_list_flag:
                            continue
                        else:
                            tmp_count += 1
                            if tmp_first_break_index == -1:
                                tmp_first_break_index = i
                            if tmp_count < tmp_judge_size:
                                continue
                            flag_second = True
                            break_index = tmp_first_break_index if tmp_first_break_index != -1 else (i-tmp_count)
                            break

                if flag_first and flag_second and deal_title_map:
                    break
            else:
                for i, title in enumerate(title_list):
                    if self.is_noresult(title):
                        continue
                    if i >= 0:
                        if title not in deal_title_map:
                            deal_title_map[title] ={'count':0, 'index_list':[], 'score':0}
                        deal_title_map[title]['count'] += 1
                        deal_title_map[title]['index_list'].append(index)
                        deal_title_map[title]['score'] += (100-index)*(100-i)

        deal_title_map_new = {}
        for index, item in enumerate(history_data[1:break_index]):
            title_list, timestamp, data = item
            for i, title in enumerate(title_list):
                if self.is_noresult(title):
                    continue
                if i == 0:
                    if title not in deal_title_map_new:
                        deal_title_map_new[title] ={'count':0, 'index_list':[]}
                    deal_title_map_new[title]['count'] += 1
                    deal_title_map_new[title]['index_list'].append(index)

        ########### New Deal Custom Result Add Count ###########
        ret_data = None
        duration_dict = {}
        duration = 0
        if break_index > 0 and deal_title_map_new:
            new_multi_title_map = {}
            for ii in range(1, break_index):
                title_list = history_data[ii][0]
                for iii, title in enumerate(title_list):
                    if self.is_noresult(title):
                        continue
                    if title not in new_multi_title_map:
                        new_multi_title_map[title] = {'count':0, 'index_list':[], 'score':0}
                    new_multi_title_map[title]['count'] += 1
                    new_multi_title_map[title]['index_list'].append(ii)
                    new_multi_title_map[title]["score"] += (100-ii)*(100-iii)

            tmp_count_map = {}
            sorted_title_list = sorted(new_multi_title_map.items(), key = lambda x:x[1]['count'], reverse = True)
            for sitem in sorted_title_list:
                sitem_title, sitem_map = sitem
                sitem_count = sitem_map["count"]
                sitem_min_index = min(sitem_map["index_list"])
                sitem_max_index = max(sitem_map["index_list"])
                sitem_score = sitem_map["score"]
                if sitem_count not in tmp_count_map:
                    tmp_count_map[sitem_count] = []
                tmp_count_map[sitem_count].append((sitem_title, sitem_min_index, sitem_max_index, sitem_score))


            first_item_flag = True
            for scount in sorted(tmp_count_map.keys(), reverse=True):
                count_list = sorted(tmp_count_map[scount], key = lambda x:x[3], reverse=True)#同一个scount,按照score进行排序
                for ditem in count_list:
                    dtitle, dmin_index, dmax_index, score= ditem
                    from_data = history_data[dmin_index][2]
                    if first_item_flag:
                        first_item_flag = False
                        ret_data = copy.deepcopy(from_data)
                        ret_data["result"]["metadata"]["custom_files"] = []
                    self.custom_result_append(ret_data, dtitle, from_data, scount, new_multi_title_map) #为每个title添加count

            index_range = set()
            for title in new_multi_title_map:
                index_range |= set(new_multi_title_map[title]['index_list'])
            min_index = min(index_range)
            max_index = max(index_range)
            duration_dict = self.compute_played_duration(history_data, min_index, max_index, True, "custom")

            self.remove_next_result_from_now_result_list(history_data, ret_data, max_index)

        if ret_data:
            duration = duration_dict["duration"]
            duration_accurate = duration_dict["duration_accurate"]
            sample_duration = duration_dict["sample_duration"]
            db_duration = duration_dict["db_duration"]
            mix_duration = duration_dict["mix_duration"]
            accurate_timestamp_utc = duration_dict["accurate_timestamp_utc"]
            ret_data['result']['metadata']['played_duration'] = abs(mix_duration)
            ret_data['result']['metadata']['timestamp_utc'] = accurate_timestamp_utc
            score, duration_sec =  self.get_score_duration_ms(ret_data)
            if ret_data['result']['metadata']['played_duration'] > duration_sec+10:
                ret_data['result']['metadata']['played_duration'] = duration_sec+int(random.random()*5)
            if score < 100 and ret_data['result']['metadata']['played_duration'] <= self._delay_custom_played_duration_min:
                ret_data = None

        ########### cut history_data #############
        if break_index>=0:
            cut_index = break_index
            for i, item in enumerate(history_data[break_index:]):
                if self.is_noresult(item[0][0]):
                    cut_index = break_index + i + 1
                else:
                    break
            cut_index = cut_index - 1 if cut_index >= 1 else cut_index
            history_data = history_data[cut_index:]

            reverse_index = -1
            for i, item in enumerate(history_data[::-1]):
                if self.is_noresult(item[0][0]):
                    reverse_index = i
                    continue
                else:
                    break

            if reverse_index != -1:
                new_cut_index = -1
                reverse_index = len(history_data) - reverse_index - 1
                if reverse_index in [0, 1]:
                    history_data = [history_data[-1]]
                else:
                    pass

            self._delay_custom[stream_id] = history_data
        return ret_data


    def deal_delay_history(self, data):
        """
        对音乐大库数据新的delay过滤算法，该算法不删除单个出现的结果，按照deal_delay_custom修改
        """
        stream_id = data.get("stream_id")
        timestamp = data.get("timestamp")
        raw_title = self.get_mutil_result_title(data, 'music', 1)[0]
        sim_title = self.tryStrSub(raw_title)
        if stream_id not in self._delay_music:
            self._delay_music[stream_id] = [(raw_title, sim_title[0], timestamp, data)]
        else:
            self._delay_music[stream_id].append((raw_title, sim_title[0], timestamp, data))

        if len(self._delay_music[stream_id]) > self._delay_list_max_num :
            return self.runDelayX_for_music_delay2(stream_id)
        else:
            return None

    def remove_next_result_from_now_result_list_for_music_delay2(self, history_data, ret_data, max_index):
        #Just for music delay2 filter
        try:
            if ret_data and len(history_data) >= max_index+2:
                raw_title, sim_title, timestamp, next_data = history_data[max_index + 1]
                if next_data:
                    next_title_list = self.get_mutil_result_title(next_data, 'music', 1)
                    next_title_set = set(next_title_list)
                    new_ret_music = []
                    for index, item in enumerate(ret_data["result"]["metadata"]["music"]):
                        if index == 0 or (item["title"] not in next_title_set):
                            new_ret_music.append(item)
                    if new_ret_music:
                        ret_data["result"]["metadata"]["music"] = new_ret_music
        except Exception as e:
            self._dlog.logger.error("Error@remove_next_result_from_now_result_list_for_music_delay2", exc_info=True)

    def result_append_for_music_delay2(self, ret_data, title, from_data):
        try:
            new_title = self.tryStrSub(title)[0]
            ret_title_set = set()
            for item in ret_data['result']['metadata']['music']:
                sim_title = self.tryStrSub(item['title'])[0]
                ret_title_set.add(sim_title.lower().strip())

            for item in from_data['result']['metadata']['music']:
                from_title = item['title']
                sim_from_title = self.tryStrSub(from_title)[0]
                sim_from_title_lower = sim_from_title.lower().strip()
                if sim_from_title_lower == new_title.lower().strip() and sim_from_title_lower not in ret_title_set:
                    ret_data['result']['metadata']['music'].append(item)
                    ret_title_set.add(sim_from_title_lower)
        except Exception as e:
            self._dlog.logger.error("Error@result_append_for_music_delay2", exc_info=True)

    def get_music_score(self, ret_data, itype="music"):
        try:
            score = 0
            result = ret_data["result"]
            if "metadata" in result:
                if itype == "music":
                    if "music" in result["metadata"]:
                        m = result["metadata"]["music"][0]
                        score = m["score"]
                if itype == "custom":
                    if "custom_files" in result["metadata"]:
                        m = result["metadata"]["custom_files"][0]
                        score = m["score"]
        except Exception as e:
            self._dlog.logger.error("Error@get_score, data:{0}".format(ret_data), exc_info=True)
        return score

    def get_music_data_offset(self, data):
        try:
            ret = {
                "monitor_len":0,
                "duration_ms":0,
                "s_begin_ms":0,
                "s_end_ms":0,
                "d_begin_ms":0,
                "d_end_ms":0
            }
            result = data.get("result")
            monitor_len = data.get("monitor_seconds", 10)
            ret["monitor_len"] = monitor_len
            if result and "metadata" in result and "music" in result["metadata"]:
                fitem = result["metadata"]["music"][0]
                ret["duration_ms"] = int(fitem["duration_ms"])
                ret["s_begin_ms"] = int(fitem["sample_begin_time_offset_ms"])
                ret["s_end_ms"] = int(fitem["sample_end_time_offset_ms"])
                ret["d_begin_ms"] = int(fitem["db_begin_time_offset_ms"])
                ret["d_end_ms"] = int(fitem["db_end_time_offset_ms"])
                return ret
        except Exception as e:
            self._dlog.logger.error("Error@get_music_data_offset, error_data:{0}".format(data), exc_info=True)
        return None

    def check_if_continuous(self, index1, index2, data1, data2):
        """
        前提：首先是计算出只有两个相似的结果，如果大于等于这两个结果，不进行判断
        测试这两个结果是不是连续的结果，两个结果acrid相同，判断是否有上下文，时间的先后关系，
        因为像radioairplay的tv流里面, 18432, 有很多短的音频播放, 但都是互为独立的
        """
        try:
            is_cont = True
            ret1 = self.get_music_data_offset(data1)
            ret2 = self.get_music_data_offset(data2)
            timestamp1 = datetime.datetime.strptime(data1["timestamp"], "%Y-%m-%d %H:%M:%S")
            timestamp2 = datetime.datetime.strptime(data2["timestamp"], "%Y-%m-%d %H:%M:%S")
            diff_sec = (timestamp2 - timestamp1).total_seconds()
            monitor_len = ret1["monitor_len"]
            if ret1 and ret2:
                dur1 = ret1["d_end_ms"] - ret1["d_begin_ms"]
                dur2 = ret2["d_end_ms"] - ret2["d_begin_ms"]
                dur1 = dur1 if dur1 > 0 else 0
                dur2 = dur2 if dur2 > 0 else 0
                ret1_s_end = ret1["s_end_ms"]
                ret2_s_begin = ret2["s_begin_ms"]
                if index1+1 == index2 and abs(monitor_len*1000 - ret1_s_end) < 1000 and abs(ret2_s_begin) < 1000 and diff_sec < monitor_len*2:
                    pass
                else:
                    ifirst, iend = max(ret1["d_begin_ms"], ret2["d_begin_ms"]), min(ret1["d_end_ms"], ret2["d_end_ms"])
                    inter_dur = iend - ifirst
                    if inter_dur > 0:
                        min_dur = min(dur1, dur2) if min(dur1, dur2) > 0 else max(dur1, dur2)
                        if min_dur > 0:
                            inter_rate = (inter_dur*1.0/min_dur)
                            if inter_dur >=2 and inter_rate >=0.8:
                                is_cont = False
        except Exception as e:
            self._dlog.logger.error("Error@check_if_continuous", exc_info=True)
        return is_cont

    def runDelayX_for_music_delay2(self, stream_id):
        """
        该算法通过修改runDelayX_custom而来, 用于不删除出现的单个结果
        """
        history_data = self._delay_music[stream_id]
        judge_zero_or_latter = True

        overflow_flag = False
        if len(history_data) >= self._delay_list_threshold:
            self._dlog.logger.error("delay_music_2[{0}] list num({1}) over threshold {2}".format(stream_id, len(history_data), self._delay_list_threshold))
            self._dlog.logger.error("delay_music_2[{0}] data: \n{1}".format(stream_id, '\n'.join(["{0}: {1}".format(i, str(item[:-1])) for i,item in enumerate(history_data[:5])])))

            history_data = history_data[-(self._delay_list_threshold-1):]
            overflow_flag = True

            history_data_len = len(history_data)
            for ii in range((history_data_len-1), 0, -1):
                if self.is_noresult(history_data[-ii][0][0] ):
                    continue
                else:
                    history_data = history_data[-(ii+1):]
                    break

        first_not_noresult_index = -1
        for index, item in enumerate(history_data):
            if index == 0:
                continue
            if self.is_noresult(item[0]):
                first_not_noresult_index = index
            else:
                break
        if first_not_noresult_index != -1:
            history_data = history_data[first_not_noresult_index:]
            self._delay_music[stream_id] = history_data
            return None

        ########## Get Break Index ##########
        deal_title_map = {} #key:title, value:{'count':0, 'index_list':[]}
        break_index = 0

        for index, item in enumerate(history_data[1:]):
            index += 1
            raw_title, sim_title, timestamp, data = item
            if index!=1:
                flag_first = True
                flag_second = True
                if sim_title in deal_title_map:
                    flag_first = False
                if flag_first:
                    tmp_all_len = len(history_data)
                    tmp_count = 0
                    tmp_first_break_index = -1
                    tmp_judge_size = self.delay_dynamic_judge_size(deal_title_map, history_data, "music")
                    for i in range(index, tmp_all_len):
                        next_raw_title, next_sim_title, next_timestamp, next_data = history_data[i]
                        tmp_list_flag = False
                        if next_sim_title in deal_title_map:
                            tmp_list_flag = True
                            tmp_count = 0
                            tmp_first_break_index = -1
                        if tmp_list_flag:
                            continue
                        else:
                            tmp_count += 1
                            if tmp_first_break_index == -1:
                                tmp_first_break_index = i
                            if tmp_count < tmp_judge_size:
                                continue
                            flag_second = True
                            break_index = tmp_first_break_index if tmp_first_break_index != -1 else i   #标记断点位置
                            break

                if flag_first and flag_second and deal_title_map:
                    if break_index >0:
                        for iii in range(index, break_index):
                            tmp_raw_title, tmp_sim_title, tmp_timestamp, tmp_data = history_data[iii]
                            if self.is_noresult(tmp_sim_title):
                                continue
                            if tmp_sim_title in deal_title_map:
                                deal_title_map[tmp_sim_title]['count'] += 1
                                deal_title_map[tmp_sim_title]['index_list'].append(iii)
                        sorted_dtitle = sorted(deal_title_map.items(), key = lambda x:x[1]['count'], reverse = True)
                        sorted_fitem_title, sorted_fitem_map = sorted_dtitle[0]
                        sfm_count = sorted_fitem_map["count"]
                        if sfm_count in [2, 3] or ((3 < sfm_count <= 10) and sfm_count < (break_index - index)):
                            cfirst_index, csecond_index = sorted(sorted_fitem_map["index_list"])[:2]
                            is_cont = self.check_if_continuous(cfirst_index, csecond_index, history_data[cfirst_index][3], history_data[csecond_index][3])
                            if not is_cont:
                                judge_zero_or_latter = False
                                break_index = cfirst_index + 1
                                deal_title_map = {sorted_fitem_title:{'count':1, 'index_list':[cfirst_index]}}
                                self._dlog.logger.warn("Stream_id:{0}, Find two not continuous.Title:{1}, Ftime:{2}, Index:[{3}, {4}]".format(stream_id, sorted_fitem_title, history_data[cfirst_index][2], cfirst_index, csecond_index))
                    break

            if self.is_noresult(sim_title):
                continue
            if sim_title not in deal_title_map:
                deal_title_map[sim_title] ={'count':0, 'index_list':[]}
            deal_title_map[sim_title]['count'] += 1
            deal_title_map[sim_title]['index_list'].append(index)

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

            from_data_index = None
            for scount in sorted(tmp_count_map.keys(), reverse=True):
                count_list = sorted(tmp_count_map[scount], key = lambda x:x[1])
                for ditem in count_list:
                    dtitle, dindex = ditem
                    from_data = history_data[dindex][3]
                    if first_item_flag:
                        first_item_flag = False
                        from_data_index = dindex
                        ret_data = copy.deepcopy(from_data)
                        ret_data["result"]["metadata"]["music"] = []

                    self.result_append_for_music_delay2(ret_data, dtitle, from_data)

            if ret_data and len(ret_data["result"]["metadata"]["music"]) == 0:
                self._dlog.logger.error("runDelayX_for_music_delay2.music_len is zero, stream_id:{0}, data:{1}".format(stream_id, ret_data))
                ret_data = None

            index_range = set()
            for title in deal_title_map:
                index_range |= set(deal_title_map[title]['index_list'])
            min_index = min(index_range)
            max_index = max(index_range)
            duration_dict = self.compute_played_duration(history_data, min_index, max_index, judge_zero_or_latter, "music")

            self.remove_next_result_from_now_result_list_for_music_delay2(history_data, ret_data, max_index)

        if ret_data:
            duration = duration_dict["duration"]
            duration_accurate = duration_dict["duration_accurate"]
            sample_duration = duration_dict["sample_duration"]
            db_duration = duration_dict["db_duration"]
            mix_duration = abs(duration_dict["mix_duration"])
            accurate_timestamp_utc = duration_dict["accurate_timestamp_utc"]
            ret_data['result']['metadata']['played_duration'] = mix_duration
            ret_data['result']['metadata']['timestamp_utc'] = accurate_timestamp_utc
            score = self.get_music_score(ret_data)
            if score > 0:
                if mix_duration < 5 and score < 85:
                    ret_data = None
            if ret_data and mix_duration <= self._delay_music_played_duration_min:
                ret_data = None

        if break_index>=0:
            cut_index = break_index
            for i, item in enumerate(history_data[break_index:]):
                if self.is_noresult(item[0][0]):
                    cut_index = break_index + i + 1
                else:
                    break
            cut_index = cut_index - 1 if cut_index >= 1 else cut_index
            history_data = history_data[cut_index:]

            reverse_index = -1
            for i, item in enumerate(history_data[::-1]):
                if self.is_noresult(item[0][0]):
                    reverse_index = i
                    continue
                else:
                    break

            if reverse_index != -1:
                new_cut_index = -1
                reverse_index = len(history_data) - reverse_index - 1
                if reverse_index in [0, 1]:
                    history_data = []
                else:
                    pass

            if judge_zero_or_latter == False and len(history_data) > 0:
                if history_data[0][0] != NORESULT:
                    tmp_t, sim_tmp_t, tmp_timestamp, tmp_data = history_data[0]
                    if tmp_data and "status" in tmp_data["result"]:
                        tmp_data["result"]["status"]["code"] = 1001
                        history_data[0] = (NORESULT, NORESULT, tmp_timestamp, tmp_data)
            self._delay_music[stream_id] = history_data


        if overflow_flag and ret_data:
            ret_data["overflow"] = overflow_flag

        return ret_data
