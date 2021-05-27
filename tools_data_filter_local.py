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
sys.setdefaultencoding('utf8')

NORESULT = 'noResult'

class ResultFilter(object):

    def __init__(self, dlog, log_dir):
        self.dlog = dlog
        self.log_dir = log_dir
        self.real_music = {}
        self.real_music_list_num = 3
        self.real_custom = {}
        self.real_custom_list_num = 3
        self.real_custom_valid_interval = 5*60
        self.real_music_score_threshold = 50
        self.real_music_duration_threshold = 3
        self.delay_music = {}
        self.delay_music_last_result = {}
        self.delay_music_check_size = 5
        self.delay_music_interval_threshold = 2*60
        self.delay_custom = {}
        self.delay_custom_check_size = 200
        self.delay_custom_played_duration_min = 2
        self.delay_music_played_duration_min = 2
        self.delay_list_max_num = 35
        self.delay_list_threshold = 70

    def is_noresult(self, ins):
        ins_type = type(ins)
        if ins_type == str or ins_type == unicode:
            if ins.strip() in ['noResult', 'noresult', NORESULT]:
                return True
            else:
                return False
        elif type(ins_type) == dict:
            if 'status' in ins and ins['status']['code'] != 0:
                return True
            else:
                return False

    def get_datetime_diff(self, t1, t2, fmt='%Y-%m-%d %H:%M:%S'):
        tt1, tt2 = (t1, t2) if t1 > t2 else (t2, t1)
        return (datetime.datetime.strptime(tt1, fmt) - datetime.datetime.strptime(tt2, fmt)).total_seconds()

    def change_timestamp(self, timestamp, value, value_type, fmt='%Y-%m-%d %H:%M:%S'):
        rtobj = datetime.datetime.strptime(timestamp, fmt)
        if value_type == 'seconds':
            return (rtobj + relativedelta(seconds=value)).strftime(fmt)
        elif value_type == 'minutes':
            return (rtobj + relativedelta(minutes=value)).strftime(fmt)
        elif value_type == 'hours':
            return (rtobj + relativedelta(hours=value)).strftime(fmt)
        elif value_type == 'months':
            return (rtobj + relativedelta(months=value)).strftime(fmt)
        else:
            return None

    def get_sim_title(self, try_str):
        if self.is_noresult(try_str):
            return try_str, False
        sub_str = tools_str_sim.str_sub(try_str)
        if len(sub_str) > 0:
            return sub_str, True
        return try_str, False

    def result_brief(self, data, itype='music', isize=1):
        try:
            blist = []
            bjinfo = {
                'title': NORESULT,
                'sim_title': NORESULT,
                'acrid': NORESULT,
                'duration':0, ''
                'score': 0,
                'sample_begin': 0, # sample_begin_time_offset_ms
                'sample_end': 0, # sample_end_time_offset_ms
                'db_begin': 0, # db_begin_time_offset_ms
                'db_end': 0, # db_end_time_offset_ms
                'timestamp_utc': '',
                'monitor_seconds': 10,
                'audio_duration': 10,
            }
            result = data['result']
            if self.is_noresult(result):
                return [bjinfo]
            elif result['status']['code'] == 0:
                key = 'music' if itype == 'music' else 'custom_files'
                if 'metadata' in result and key in result['metadata']:
                    for index, item in enumerate(result['metadata'][key]):
                        tmp_bjinfo = copy.deepcopy(bjinfo)
                        tmp_bjinfo['title'] = str(item['title'])
                        tmp_bjinfo['sim_title'] = str(self.get_sim_title(item['title'])[0]) if itype == 'music' else str(item['acrid'])
                        tmp_bjinfo['acrid'] = item['acrid']
                        tmp_bjinfo['duration'] = round(int(item.get('duration_ms', 0))*1.0/1000, 2)
                        tmp_bjinfo['score'] = int(item.get('score', 0))
                        tmp_bjinfo['sample_begin'] = round(int(item['sample_begin_time_offset_ms'])*1.0/1000, 2)
                        tmp_bjinfo['sample_end'] = round(int(item['sample_end_time_offset_ms'])*1.0/1000, 2)
                        tmp_bjinfo['db_begin'] = round(int(item['db_begin_time_offset_ms'])*1.0/1000, 2)
                        tmp_bjinfo['db_end'] = round(int(item['db_end_time_offset_ms'])*1.0/1000, 2)
                        tmp_bjinfo['timestamp_utc'] = result['metadata']['timestamp_utc']
                        tmp_bjinfo['monitor_seconds'] = data['monitor_seconds']
                        tmp_bjinfo['audio_duration'] = data['audio_duration'] if 'audio_duration' in data else data['monitor_seconds']
                        blist.append(tmp_bjinfo)
                    return blist[:isize]
        except Exception as e:
            self.dlog.logger.error('Error@get_result_brief, data: {0}, itype: {1}, {2}'.format(data, itype, isize), exc_info=True)
        return [bjinfo]

    def get_mutil_result_title(self, data, itype='music', isize=1):
        try:
            title_list = []
            item_list = self.result_brief(data, itype, isize)
            title_list = [it['title'] for it in item_list]
        except Exception as e:
            self.dlog.logger.error('Error@get_multi_result_title', exc_info=True)
        return title_list

    def get_mutil_result_acrid(self, data, itype='music', isize=1):
        try:
            title_list = []
            item_list = self.result_brief(data, itype, isize)
            title_list = [it['acrid'] for it in item_list]
        except Exception as e:
            self.dlog.logger.error('Error@get_multi_result_acrid', exc_info=True)
        return title_list

    def real_custom_check_title(self, stream_id, title, timestamp_tobj):
        now_timestamp = timestamp_tobj
        if stream_id not in self.real_custom:
            self.real_custom[stream_id] = [[('','')], '']

        if len(self.real_custom[stream_id][0]) > self.real_custom_list_num:
            self.real_custom[stream_id][0] = self.real_custom[stream_id][0][-self.real_custom_list_num:]
            his_list_num = self.real_custom_list_num
        else:
            his_list_num = len(self.real_custom[stream_id][0])

        for i in range(his_list_num-1, -1, -1):
            if self.real_custom[stream_id][0][i][0] == title:
                his_timestamp = self.real_custom[stream_id][0][i][1]
                his_time_obj = datetime.datetime.strptime(his_timestamp, '%Y-%m-%d %H:%M:%S')
                if (now_timestamp - his_time_obj).total_seconds() <= self.real_custom_valid_interval:
                    return True
            if self.is_noresult(title):
                break
        return False

    def real_music_if_false_positive(self, stream_id, result_brief):
        try:
            false_positive = False
            title, score, timestamp_utc = result_brief['title'], result_brief['score'], result_brief['timestamp_utc']
            if self.is_noresult(title):
                return false_positive
            sample_duration = result_brief['sample_end'] - result_brief['sample_begin']
            if result_brief['score'] < self.real_music_score_threshold and sample_duration <= self.real_music_duration_threshold:
                false_positive = True
                self.dlog.logger.warning('Warning@real_music_if_false_positive, not return.ID:{0}, title:{1}, score:{2} < {3}, sample_duration:{4}<{5}, timestamp:{6}'.format(stream_id, title, score, self.real_music_score_threshold, sample_duration, self.real_music_duration_threshold, timestamp_utc ))
        except Exception as e:
            self.dlog.logger.error('Error@real_music_if_false_positive:{0}'.format(data), exc_info=True)
        return false_positive

    def real_result_sim(self, idx, curr_title, his_title, stream_id):
        if not curr_title or not his_title:
            return False
        sim, detail = tools_str_sim.str_sim(curr_title, his_title)
        if not sim and not self.is_noresult(curr_title) and not self.is_noresult(his_title):
            self.dlog.logger.info('Sim@StreamID: {0}, CurrTitle: {1}, HisTitle: {2}({3}), Sim: {4}'.format(str(stream_id), curr_title, his_title, str(idx), str(detail)))
        return sim

    def real_check_same(self, curr_title, stream_id):
        self.real_music[stream_id] = self.real_music.get(stream_id, [[''], ''])
        if len(self.real_music[stream_id][0]) > self.real_music_list_num:
            self.real_music[stream_id][0] = self.real_music[stream_id][0][-self.real_music_list_num:]
            his_max = self.real_music_list_num
        else:
            his_max = len(self.real_music[stream_id][0])
        for i in range(his_max-1, -1, -1):
            if self.real_result_sim(i, curr_title, self.real_music[stream_id][0][i], stream_id):
                return True
            if self.is_noresult(curr_title):
                break
        return False

    def real_update_timestamp_utc(self, data, itype='music'):
        try:
            new_data = copy.deepcopy(data)
            result_brief = self.result_brief(new_data, itype, 1)[0]
            if not self.is_noresult(result_brief['sim_title']):
                new_timestamp_utc = self.change_timestamp(result_brief['timestamp_utc'], result_brief['sample_begin'], 'seconds')
                new_data['result']['metadata']['timestamp_utc'] = new_timestamp_utc
        except Exception as e:
            self.dlog.logger.error('Error@update_real_result_timestamp_utc', exc_info=True)
        return new_data

    def delay_dynamic_judge_size(self, deal_title_map, history_data, itype):
        try:
            judge_size = 5
            sim_title = sorted(deal_title_map.items(), key=lambda x:x[1]['p_weight'], reverse=True)[0][0]
            if sim_title and sim_title.lower().startswith('first of the year'):
                return 1

            index = deal_title_map[sim_title]['index_list'][-1]
            title_list, timestamp, ret_data = history_data[index]

            monitor_len = ret_data.get('monitor_seconds', 10)

            result_brief = None
            for t_index, item_brief in enumerate(title_list):
                if item_brief['sim_title'] == sim_title or item_brief['acrid'] == sim_title:
                    result_brief = item_brief

            if result_brief:
                duration = result_brief['duration']
                db_end_offset = result_brief['db_end']

                if db_end_offset > 0 and db_end_offset < duration:
                    judge_size = abs(int(math.ceil((duration - db_end_offset)*1.0/monitor_len))) + 1
                if judge_size > 10:
                    judge_size = 10
                if judge_size <= 3:
                    judge_size = 3 if itype == 'music' else 1
        except Exception as e:
            self.dlog.logger.error('Error@delay_dynamic_judge_size', exc_info=True)
        return judge_size

    def delay_fill_ret_data(self, sim_title_set, sorted_title_list, history_data, itype='music'):
        try:
            ret_data = None
            init_ret_data = True
            key_value = 'music' if itype == 'music' else 'custom_files'
            acrid_data_map = {}
            for sitem in sorted_title_list:
                sitem_title, sitem_map = sitem
                if sitem_title not in sim_title_set:
                    continue
                for index in sitem_map['index_list']:
                    title_list, timestamp, tdata = history_data[index]
                    if init_ret_data:
                        ret_data = copy.deepcopy(tdata)
                        ret_data['result']['metadata'][key_value] = []
                        init_ret_data = False
                    if 'metadata' in tdata['result'] and key_value in tdata['result']['metadata']:
                        r_len = len(tdata['result']['metadata'][key_value])
                        for t_index, item_brief in enumerate(title_list):
                            if item_brief['sim_title'] == sitem_title or item_brief['acrid'] == sitem_title:
                                if t_index <= r_len - 1:
                                    if item_brief['acrid'] not in acrid_data_map:
                                        acrid_data_map[item_brief['acrid']] = {
                                            'score': 0,
                                            'count': 0,
                                            'p_weight': 0,
                                            'data': tdata['result']['metadata'][key_value][t_index]
                                        }
                                    score = item_brief['score']
                                    acrid_data_map[item_brief['acrid']]['score'] += score
                                    acrid_data_map[item_brief['acrid']]['count'] += 1
                                    acrid_data_map[item_brief['acrid']]['p_weight'] += (201 - t_index)*(int(score) if int(score) > 0 else 1)
            sort_acrid_data_map = sorted(acrid_data_map.items(), key=lambda x:x[1]['p_weight'], reverse=True)
            for (acrid, acrid_info) in sort_acrid_data_map:
                acrid_info['data']['score'] = math.ceil(acrid_info['score']/acrid_info['count'])
                if itype == 'custom':
                    acrid_info['data']['count'] = acrid_info['count']
                if ret_data:
                    ret_data['result']['metadata'][key_value].append(acrid_info['data'])

            if itype == 'music' and ret_data is not None and len(ret_data['result']['metadata']['music']) > 3:
                ret_data['result']['metadata']['music'] = ret_data['result']['metadata']['music'][:3]
            if itype == 'custom' and ret_data is not None and len(ret_data['result']['metadata']['custom_files']) > 20:
                ret_data['result']['metadata']['custom_files'] = ret_data['result']['metadata']['custom_files'][:20]

        except Exception as e:
            self.dlog.logger.error('Error@delay_fill_ret_data', exc_info=True)
        return ret_data

    def get_result_brief_from_title_list(self, sim_title, title_list):
        for item in title_list:
            if item['sim_title'] == sim_title or item['acrid'] == sim_title:
                return item
        return None

    def delay_check_offset_overlaps(self, brief1, brief2):
        if (brief1['sample_begin'] <= brief2['sample_begin'] < brief1['sample_end']) or (brief1['sample_begin'] < brief2['sample_end'] <= brief1['sample_end']):
            return True
        else:
            return False

    def delay_check_result_sim(self, deal_title_map, title_list):
        try:
            yes_index, yes_result_brief, sim_title_set = -1, None, set()
            for i, result_brief in enumerate(title_list):
                if result_brief['sim_title'] in deal_title_map:
                    yes_index, yes_result_brief = i, result_brief
                    break
            if yes_index != -1:
                for i, result_brief in enumerate(title_list):
                    if i != yes_index:
                        if self.delay_check_offset_overlaps(yes_result_brief, result_brief):
                            sim_title_set.add(result_brief['sim_title'])
        except Exception as e:
            self.dlog.logger.error('Error@delay_check_result_sim', exc_info=True)
        return sim_title_set

    def delay_find_interset_title(self, sorted_title_list, history_data):
        try:
            sim_title_set, sim_index_set = set(), set()
            main_title, main_map = sorted_title_list[0]
            sim_title_set.add(main_title)
            sim_index_set |= set(main_map['index_list'])
            main_start_index, main_end_index = main_map['index_list'][0], main_map['index_list'][-1]
            main_start_result_brief = self.get_result_brief_from_title_list(main_title, history_data[main_start_index][0])
            main_end_result_brief = self.get_result_brief_from_title_list(main_title, history_data[main_end_index][0])

            for (next_title, next_map) in sorted_title_list[1:]:
                for (main_index, main_result_brief) in [(main_start_index, main_start_result_brief), (main_end_index, main_end_result_brief)]:
                    if main_index in next_map['index_list']:
                        next_result_brief = self.get_result_brief_from_title_list(next_title, history_data[main_index][0])
                        if self.delay_check_offset_overlaps(main_result_brief, next_result_brief):
                            sim_title_set.add(next_title)
                            sim_index_set |= set(next_map['index_list'])
        except Exception as e:
            self.dlog.logger.error('Error@delay_find_interset_title', exc_info=True)
        return sim_title_set, sim_index_set

    def delay_check_if_break(self, index1, result_brief1, index2, result_brief2):
        try:
            is_break = False
            diff_db = result_brief2['db_end'] - result_brief1['db_begin']
            if diff_db <= 0:
                return is_break
            monitor_len = result_brief1['audio_duration'] if result_brief1['audio_duration'] > 8 else 10
            A1 = self.change_timestamp(result_brief1['timestamp_utc'], result_brief1['sample_begin'], 'seconds')
            A2 = self.change_timestamp(result_brief2['timestamp_utc'], result_brief2['sample_begin'], 'seconds')
            B1 = self.get_datetime_diff(A1, A2)
            B2 = (index2 - index1 - 1)*monitor_len + int(diff_db)
            B3 = int(diff_db)
            if abs(B3 - B1) <= 15:
                is_break = False
            elif abs(B2 - B1) <= 10:
                is_break = True
        except Exception as e:
            self.dlog.logger.error('Error@delay_check_if_break', exc_info=True)
        return is_break

    def delay_check_if_continuous(self, index1, result_brief1, index2, result_brief2):
        try:
            is_cont = True

            if index1 == index2:
                return is_cont

            diff_sec = self.get_datetime_diff(result_brief1['timestamp_utc'], result_brief2['timestamp_utc'])
            monitor_len1 = result_brief1['audio_duration']
            monitor_len2 = result_brief2['audio_duration']

            dur1 = result_brief1['db_end'] - result_brief1['db_begin']
            dur2 = result_brief2['db_end'] - result_brief2['db_begin']
            dur1 = dur1 if dur1 > 0 else 0
            dur2 = dur2 if dur2 > 0 else 0
            if index1+1 == index2 and abs(monitor_len1 - result_brief1['sample_end']) < 2.5 and abs(result_brief2['sample_begin']) < 2.5 and diff_sec < (monitor_len1 + monitor_len2):
                pass
            else:
                ifirst, iend = max(result_brief1['db_begin'], result_brief2['db_begin']), min(result_brief1['db_end'], result_brief2['db_end'])
                inter_dur = iend - ifirst
                if inter_dur > 0:
                    min_dur = min(dur1, dur2) if min(dur1, dur2) > 0 else max(dur1, dur2)
                    if min_dur > 0:
                        inter_rate = (inter_dur*1.0/min_dur)
                        if inter_dur >=2 and inter_rate >=0.8:
                            is_cont = False
        except Exception as e:
            self.dlog.logger.error('Error@delay_check_if_continuous', exc_info=True)
        return is_cont

    def delay_check_index1_individual_result(self, sim_title_set, history_data):
        try:
            is_indiv, indiv_list, sim_list, new_sim_title_set, new_sorted_title_list = False, [], [], set(), []
            for index, result_brief in enumerate(history_data[1][0]):
                if result_brief['sim_title'] not in sim_title_set and result_brief['acrid'] not in sim_title_set:
                    indiv_list.append((index, result_brief))
                else:
                    sim_list.append((index, result_brief))

            for (in_index, indiv_rbrief) in indiv_list:
                for (sim_index, sim_rbrief) in sim_list:
                    if indiv_rbrief['sample_end'] < sim_rbrief['sample_begin'] and (indiv_rbrief['sample_end'] - indiv_rbrief['sample_begin']) > 3:
                        is_indiv = True
                        new_sim_title_set.add(indiv_rbrief['sim_title'])
                        new_sorted_title_list = [
                            (indiv_rbrief['sim_title'], {
                                'count': 1,
                                'index_list': [1],
                                'score': indiv_rbrief['score'],
                                'p_weight': indiv_rbrief['score']
                            })
                        ]
                        break
                if is_indiv:
                    break
        except Exception as e:
            self.dlog.logger.error('Error@delay_check_index1_individual_result', exc_info=True)
        return is_indiv, new_sim_title_set, new_sorted_title_list

    def delay_compute_played_duration(self, sim_title_set, start_index, end_index, history_data):
        try:
            tmp_start_index, tmp_end_index = start_index, end_index
            start_result_brief, end_result_brief = None, None
            for i, result_index in enumerate([start_index, end_index]):
                for item in history_data[result_index][0]:
                    if item['sim_title'] in sim_title_set or item['acrid'] in sim_title_set:
                        if i == 0:
                            if start_result_brief is None or (start_result_brief and start_result_brief['sample_begin'] > item['sample_begin']):
                                start_result_brief = item
                        else:
                            if end_result_brief is None or (end_result_brief and end_result_brief['sample_end'] < item['sample_end']):
                                end_result_brief = item

            if start_index == 1:
                zero_title_list = history_data[0][0]
                for i, result_brief in enumerate(zero_title_list):
                    if result_brief['sim_title'] in sim_title_set or result_brief['acrid'] in sim_title_set:
                        start_result_brief = result_brief
                        tmp_start_index = 0
                        break

            start_timestamp_utc = self.change_timestamp(start_result_brief['timestamp_utc'], math.floor(start_result_brief['sample_begin']), 'seconds')
            end_timestamp_utc = self.change_timestamp(end_result_brief['timestamp_utc'], math.ceil(end_result_brief['sample_end']), 'seconds')
            played_duration1 = self.get_datetime_diff(start_timestamp_utc, end_timestamp_utc)

            total_audio_duration = sum([ history_data[i][0][0]['audio_duration'] for i in range(tmp_start_index, tmp_end_index)])
            played_duration2 = abs(int(math.ceil(total_audio_duration - math.floor(start_result_brief['sample_begin']) + math.ceil(end_result_brief['sample_end']))))

            return { 'played_duration' : played_duration2, 'timestamp_utc': start_timestamp_utc, 'time_diff': played_duration1 }
        except Exception as e:
            self.dlog.logger.error('Error@delay_compute_played_duration', exc_info=True)
        return None

    def delay_remove_ret_info_for_next(self, sim_title_set, end_index, history_data, itype='music'):
        try:
            end_title_list, end_timestamp, end_data = history_data[end_index]
            type_key = 'music' if itype == 'music' else 'custom_files'
            if end_data and type_key in end_data['result']['metadata']:
                reserved_list = []
                for index, result_brief in enumerate(end_title_list):
                    if result_brief['sim_title'] in sim_title_set or result_brief['acrid'] in sim_title_set:
                        continue
                    else:
                        reserved_list.append(end_data['result']['metadata'][type_key][index])
                if len(reserved_list) > 0:
                    end_data['result']['metadata'][type_key] = reserved_list
                else:
                    end_data['result'] = {
                        'status': {'msg': 'No result', 'code': 1001, 'version': '1.0'},
                        'metadata': { 'timestamp_utc': end_timestamp }
                    }
                check_size = self.delay_music_check_size if itype == 'music' else self.delay_custom_check_size
                new_title_list = self.result_brief(end_data, itype, check_size)
                history_data[end_index] = (new_title_list, end_timestamp, end_data)
        except Exception as e:
            self.dlog.logger.error('Error@delay_remove_ret_info_for_next', exc_info=True)

    def run_delay_music(self, stream_id):
        history_data = self.delay_music[stream_id]

        overflow_flag = False
        if len(history_data) >= self.delay_list_threshold:
            self.dlog.logger.error('delay_music[{0}] list num({1}) over threshold {2}'.format(stream_id, len(history_data), self.delay_list_threshold))
            self.dlog.logger.error('delay_music[{0}] data: \n{1}'.format(stream_id, '\n'.join(['{0}: {1}, {2}'.format(i, item[1], item[0][0]['title']) for i,item in enumerate(history_data[:5])])))

            history_data = history_data[-(self.delay_list_threshold-1):]
            overflow_flag = True

        first_not_noresult_index = -1
        for index, item in enumerate(history_data[1:], 1):
            if self.is_noresult(item[0][0]['sim_title']):
                first_not_noresult_index = index
            else:
                break
        if first_not_noresult_index != -1:
            history_data = history_data[first_not_noresult_index:]
            self.delay_music[stream_id] = history_data
            return None

        deal_title_map = {}
        break_index = 0

        for index, item in enumerate(history_data[1:], 1):
            title_list, timestamp, data = item
            if index == 1:
                for t_index, bitem in enumerate(title_list):
                    sim_title, score = bitem['sim_title'], bitem['score']
                    if self.is_noresult(sim_title):
                        break
                    if sim_title not in deal_title_map:
                        deal_title_map[sim_title] = {'count':0, 'index_list':[], 'score':0, 'p_weight':0}
                    deal_title_map[sim_title]['count'] += 1
                    deal_title_map[sim_title]['index_list'].append(index)
                    deal_title_map[sim_title]['score'] += score
                    deal_title_map[sim_title]['p_weight'] += (100-t_index)*(int(score) if int(score) > 0 else 1)
            else:
                flag_first = True
                flag_second = False
                has_sim_title = self.delay_check_result_sim(deal_title_map, title_list)
                for t_index, bitem in enumerate(title_list):
                    sim_title, score = bitem['sim_title'], bitem['score']
                    if sim_title in deal_title_map or (has_sim_title and sim_title in has_sim_title):
                        if sim_title not in deal_title_map:
                            deal_title_map[sim_title] = {'count':0, 'index_list':[], 'score':0, 'p_weight':0}
                        deal_title_map[sim_title]['count'] += 1
                        deal_title_map[sim_title]['index_list'].append(index)
                        deal_title_map[sim_title]['score'] += score
                        deal_title_map[sim_title]['p_weight'] += (100-t_index)*(int(score) if int(score) > 0 else 1)
                        flag_first = False

                if flag_first:
                    tmp_all_len = len(history_data)
                    tmp_count = 0
                    tmp_first_break_index = -1
                    tmp_judge_size = self.delay_dynamic_judge_size(deal_title_map, history_data, 'music')
                    find_interval = False
                    find_pre_last_index = index - 1
                    find_next_sim_index = -1
                    for i in range(index, tmp_all_len):
                        next_title_list, next_timestamp, next_data = history_data[i]
                        in_list_flag = False
                        is_break = False
                        next_has_sim_title = self.delay_check_result_sim(deal_title_map, next_title_list)
                        for next_index, next_bitem in enumerate(next_title_list[:self.delay_music_check_size]):
                            next_sim_title, next_score = next_bitem['sim_title'], next_bitem['score']
                            if next_sim_title in deal_title_map or (next_has_sim_title and next_sim_title in next_has_sim_title):
                                if next_sim_title not in deal_title_map:
                                    deal_title_map[next_sim_title] = {'count':0, 'index_list':[], 'score':0, 'p_weight':0}
                                deal_title_map[next_sim_title]['count'] += 1
                                deal_title_map[next_sim_title]['index_list'].append(i)
                                deal_title_map[next_sim_title]['score'] += next_score
                                deal_title_map[next_sim_title]['p_weight'] += (100-next_index)*(int(next_score) if int(next_score) > 0 else 1)
                                in_list_flag = True
                                tmp_count = 0
                                tmp_first_break_index = -1
                                if find_interval == True:
                                    find_interval = False
                                    find_next_sim_index = i
                                    if find_next_sim_index - find_pre_last_index - 1 >= 6:
                                        pre_result_brief = history_data[find_pre_last_index][0][0]
                                        is_break = self.delay_check_if_break(find_pre_last_index, pre_result_brief, find_next_sim_index, next_bitem)
                                        if is_break:
                                            break_index = find_pre_last_index + 1
                                            break
                                        else:
                                            find_interval = True
                                            find_pre_last_index = i
                        if is_break:
                            break

                        if in_list_flag:
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
                    if break_index > 0:
                        break

        ret_data, duration_info = None, {}
        if break_index > 0 and deal_title_map:
            new_deal_title_map = {}
            for index in range(1, break_index):
                title_list, timestamp, data = history_data[index]
                for t_index, t_json in enumerate(title_list):
                    sim_title = t_json['sim_title']
                    score = t_json['score']
                    if self.is_noresult(sim_title):
                        continue
                    if sim_title not in new_deal_title_map:
                        new_deal_title_map[sim_title] = {'count':0, 'index_list':[], 'score':0, 'p_weight':0}
                    new_deal_title_map[sim_title]['count'] += 1
                    new_deal_title_map[sim_title]['index_list'].append(index)
                    new_deal_title_map[sim_title]['score'] += score
                    new_deal_title_map[sim_title]['p_weight'] += (100-t_index)*(int(score) if int(score) > 0 else 1)

            sorted_title_list = sorted(new_deal_title_map.items(), key = lambda x:(x[1]['count'], x[1]['p_weight'], x[1]['score']), reverse = True)

            sorted_fitem_title, sorted_fitem_map = sorted_title_list[0]
            sfm_count = sorted_fitem_map['count']
            if sfm_count in [2, 3]:
                cfirst_index, csecond_index = sorted(sorted_fitem_map['index_list'])[:2]
                cfirst_result_brief = [ tb for tb in history_data[cfirst_index][0] if tb['sim_title'] == sorted_fitem_title ][0]
                csecond_result_brief = [ tb for tb in history_data[csecond_index][0] if tb['sim_title'] == sorted_fitem_title ][0]
                is_cont = self.delay_check_if_continuous(cfirst_index, cfirst_result_brief, csecond_index, csecond_result_brief)
                if not is_cont:
                    break_index = cfirst_index + 1
                    sorted_title_list = [
                        (sorted_fitem_title, {
                            'count': 1,
                            'index_list': [cfirst_index],
                            'score': cfirst_result_brief['score'],
                            'p_weight': cfirst_result_brief['score']
                        })
                    ]
                    self.dlog.logger.warn('Stream_id:{0}, Find two not continuous.Title:{1}, Ftime:{2}, Index:[{3}, {4}]'.format(stream_id, sorted_fitem_title, cfirst_result_brief['timestamp_utc'], cfirst_index, csecond_index))

            sim_title_set, sim_index_set = self.delay_find_interset_title(sorted_title_list, history_data)

            is_indiv, new_sim_title_set, new_sorted_title_list = self.delay_check_index1_individual_result(sim_title_set, history_data)
            if is_indiv and new_sim_title_set and new_sorted_title_list:
                break_index = 1 if (1 in sim_index_set and len(sim_index_set) == 1) else 2
                sim_title_set, sim_index_set, sorted_title_list = new_sim_title_set, set([1]), new_sorted_title_list

            min_index, max_index = min(sim_index_set), max(sim_index_set)

            ret_data = self.delay_fill_ret_data(sim_title_set, sorted_title_list, history_data, itype='music')

            if ret_data and len(ret_data['result']['metadata']['music']) == 0:
                self.dlog.logger.error('run_delay_music.music_len is zero, stream_id:{0}, data:{1}'.format(stream_id, ret_data))
                ret_data = None

            duration_info = self.delay_compute_played_duration(sim_title_set, min_index, max_index, history_data)

            self.delay_remove_ret_info_for_next(sim_title_set, max_index, history_data, itype='music')

        if ret_data and duration_info:
            played_duration = abs(duration_info['played_duration'])
            timestamp_utc = duration_info['timestamp_utc']
            ret_data['result']['metadata']['played_duration'] = played_duration
            ret_data['result']['metadata']['timestamp_utc'] = timestamp_utc
            if played_duration < self.delay_music_played_duration_min:
                ret_data = None

        if break_index >= 0:
            split_index = break_index
            for i in range(break_index, len(history_data)):
                if self.is_noresult(history_data[i][0][0]['title']):
                    continue
                else:
                    split_index = i
                    break

            split_index = split_index - 1 if split_index >= 1 else split_index
            history_data = history_data[split_index:]
            self.delay_music[stream_id] = history_data

        if overflow_flag and ret_data:
            ret_data['overflow'] = overflow_flag

        return ret_data


    def run_delay_custom(self, stream_id):
        history_data = self.delay_custom[stream_id]

        overflow_flag = False
        if len(history_data) >= self.delay_list_threshold:
            self.dlog.logger.error('delay_custom[{0}] list num({1}) over threshold {2}'.format(stream_id, len(history_data), self.delay_list_threshold))
            self.dlog.logger.error('delay_custom[{0}] data: \n{1}'.format(stream_id, '\n'.join(['{0}: {1}, {2}'.format(i, item[1], item[0][0]['title']) for i,item in enumerate(history_data[:5])])))

            history_data = history_data[-(self.delay_list_threshold-1):]
            overflow_flag = True

        first_not_noresult_index = -1
        for index, item in enumerate(history_data[1:], 1):
            if len(item[0]) > 0 and self.is_noresult(item[0][0]['sim_title']):
                first_not_noresult_index = index
            else:
                break
        if first_not_noresult_index != -1:
            history_data = history_data[first_not_noresult_index:]
            self.delay_custom[stream_id] = history_data
            return None

        deal_title_map = {}
        break_index = 0

        for index, item in enumerate(history_data[1:], 1):
            title_list, timestamp, data = item
            if index == 1:
                for t_index, bitem in enumerate(title_list):
                    sim_title, score = bitem['sim_title'], bitem['score']
                    if self.is_noresult(sim_title):
                        break
                    if sim_title not in deal_title_map:
                        deal_title_map[sim_title] ={'count':0, 'index_list':[], 'score':0, 'p_weight':0}
                    deal_title_map[sim_title]['count'] += 1
                    deal_title_map[sim_title]['index_list'].append(index)
                    deal_title_map[sim_title]['score'] += score
                    deal_title_map[sim_title]['p_weight'] += (100-t_index)*(int(score) if int(score) > 0 else 1)
            else:
                flag_first = True
                flag_second = False
                for t_index, bitem in enumerate(title_list[:1]):
                    sim_title, score = bitem['sim_title'], bitem['score']
                    if sim_title in deal_title_map:
                        deal_title_map[sim_title]['count'] += 1
                        deal_title_map[sim_title]['index_list'].append(index)
                        deal_title_map[sim_title]['score'] += score
                        deal_title_map[sim_title]['p_weight'] += (100-t_index)*(int(score) if int(score) > 0 else 1)
                        flag_first = False
                if flag_first:
                    tmp_all_len = len(history_data)
                    tmp_count = 0
                    tmp_first_break_index = -1
                    tmp_judge_size = self.delay_dynamic_judge_size(deal_title_map, history_data, 'custom')
                    for i in range(index, tmp_all_len):
                        next_title_list, next_timestamp, next_data = history_data[i]
                        in_list_flag = False
                        for next_bitem in next_title_list[:self.delay_custom_check_size]:
                            if next_bitem['sim_title'] in deal_title_map:
                                in_list_flag = True
                                tmp_count = 0
                                tmp_first_break_index = -1
                        if in_list_flag:
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
                    if break_index > 0:
                        break

        ret_data, duration_info = None, {}
        if break_index > 0 and deal_title_map:
            new_deal_title_map = {}
            for ii in range(1, break_index):
                title_list, timestamp, data = history_data[ii]
                for iii, result_brief in enumerate(title_list):
                    sim_title, score = result_brief['sim_title'], result_brief['score']
                    if self.is_noresult(sim_title):
                        continue
                    if sim_title not in new_deal_title_map:
                        new_deal_title_map[sim_title] = {'count':0, 'index_list':[], 'score':0, 'p_weight':0}
                    new_deal_title_map[sim_title]['count'] += 1
                    new_deal_title_map[sim_title]['index_list'].append(ii)
                    new_deal_title_map[sim_title]['score'] += score
                    new_deal_title_map[sim_title]['p_weight'] += (201-iii)*(int(score) if int(score) > 0 else 1)

            sorted_title_list = sorted(new_deal_title_map.items(), key = lambda x:(x[1]['count'], x[1]['p_weight'], x[1]['score']), reverse = True)

            sorted_fitem_title, sorted_fitem_map = sorted_title_list[0]
            sfm_count = sorted_fitem_map['count']
            if sfm_count in [2, 3]:
                cfirst_index, csecond_index = sorted(sorted_fitem_map['index_list'])[:2]
                cfirst_result_brief = [ tb for tb in history_data[cfirst_index][0] if tb['sim_title'] == sorted_fitem_title ][0]
                csecond_result_brief = [ tb for tb in history_data[csecond_index][0] if tb['sim_title'] == sorted_fitem_title ][0]
                is_cont = self.delay_check_if_continuous(cfirst_index, cfirst_result_brief, csecond_index, csecond_result_brief)
                if not is_cont:
                    break_index = cfirst_index + 1
                    sorted_title_list = [
                        (sorted_fitem_title, {
                            'count': 1,
                            'index_list': [cfirst_index],
                            'score': cfirst_result_brief['score'],
                            'p_weight': cfirst_result_brief['score']
                        })
                    ]
                    self.dlog.logger.warn('Stream_id:{0}, Find two not continuous.Title:{1}, Ftime:{2}, Index:[{3}, {4}]'.format(stream_id, sorted_fitem_title, cfirst_result_brief['timestamp_utc'], cfirst_index, csecond_index))

            sim_title_set, sim_index_set = self.delay_find_interset_title(sorted_title_list, history_data)

            is_indiv, new_sim_title_set, new_sorted_title_list = self.delay_check_index1_individual_result(sim_title_set, history_data)
            if is_indiv and new_sim_title_set and new_sorted_title_list:
                break_index = 1 if (1 in sim_index_set and len(sim_index_set) == 1) else 2
                sim_title_set, sim_index_set, sorted_title_list = new_sim_title_set, set([1]), new_sorted_title_list

            min_index, max_index = min(sim_index_set), max(sim_index_set)

            ret_data = self.delay_fill_ret_data(sim_title_set, sorted_title_list, history_data, itype='custom')

            if ret_data and len(ret_data['result']['metadata']['custom_files']) == 0:
                self.dlog.logger.error('run_delay_custom.custom_len is zero, stream_id:{0}, data:{1}'.format(stream_id, ret_data))
                ret_data = None

            duration_info = self.delay_compute_played_duration(sim_title_set, min_index, max_index, history_data)

            self.delay_remove_ret_info_for_next(sim_title_set, max_index, history_data, itype='custom')

        if ret_data and duration_info:
            played_duration = abs(duration_info['played_duration'])
            timestamp_utc = duration_info['timestamp_utc']
            ret_data['result']['metadata']['played_duration'] = played_duration
            ret_data['result']['metadata']['timestamp_utc'] = timestamp_utc

            ret_duration_sec = int(int(ret_data['result']['metadata']['custom_files'][0]['duration_ms'])*1.0/1000)
            ret_score = int(ret_data['result']['metadata']['custom_files'][0]['score'])
            if ret_duration_sec >= 0 and played_duration > ret_duration_sec and played_duration < ret_duration_sec + 3:
                ret_data['result']['metadata']['played_duration'] = ret_duration_sec
            if ret_score < 100 and ret_data['result']['metadata']['played_duration'] <= self.delay_custom_played_duration_min:
                ret_data = None

        if break_index >= 0:
            split_index = break_index
            for i in range(break_index, len(history_data)):
                if self.is_noresult(history_data[i][0][0]['title']):
                    continue
                else:
                    split_index = i
                    break

            split_index = split_index - 1 if split_index >= 1 else split_index
            history_data = history_data[split_index:]
            self.delay_custom[stream_id] = history_data

        if overflow_flag and ret_data:
            ret_data['overflow'] = overflow_flag

        return ret_data

    def deal_real_custom(self, data):
        is_new, result = False, None
        curr_result_brief = self.result_brief(data, 'custom', 1)[0]
        curr_title = curr_result_brief['sim_title']

        stream_id, timestamp = data['stream_id'], data['timestamp']
        timestamp_tobj = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')

        if not stream_id:
            return result, is_new
        if self.is_noresult(curr_title):
            if not self.real_custom_check_title(stream_id, curr_title, timestamp_tobj):
                self.real_custom[stream_id][0].append((curr_title, timestamp))
                self.real_custom[stream_id][1] = data
                is_new, result = True, data
            else:
                is_new, result = False, None
        else:
            if self.real_custom_check_title(stream_id, curr_title, timestamp_tobj):
                is_new, result = False, self.real_custom[stream_id][1]
            else:
                self.real_custom[stream_id][0].append((curr_title, timestamp))
                self.real_custom[stream_id][1] = data
                is_new, result = True, data

        if is_new and result:
            result = self.real_update_timestamp_utc(result, 'custom')

        return result, is_new

    def deal_delay_custom(self, data):
        try:
            ret_result = None
            stream_id = data.get('stream_id')
            timestamp = data.get('timestamp')
            title_list = self.result_brief(data, 'custom', self.delay_custom_check_size)
            if stream_id not in self.delay_custom:
                self.delay_custom[stream_id] = [(title_list, timestamp, data)]
            else:
                self.delay_custom[stream_id].append((title_list, timestamp, data))

            if len(self.delay_custom[stream_id]) >= self.delay_list_max_num:
                ret_result = self.run_delay_custom(stream_id)
        except Exception as e:
            self.dlog.logger.error('Error@deal_delay_custom', exc_info=True)
        return ret_result

    def deal_real_history(self, data):
        return self.deal_real_music(data)

    def deal_real_music(self, data):
        is_new, result = False, None
        curr_result_brief = self.result_brief(data, 'music', 1)[0]
        curr_title = curr_result_brief['sim_title']
        stream_id = data.get('stream_id')
        if not stream_id:
            return result, is_new

        is_false_positive = self.real_music_if_false_positive(stream_id, curr_result_brief)
        if is_false_positive:
            return None, False

        if self.is_noresult(curr_title):
            if not self.real_check_same(curr_title, stream_id):
                self.real_music[stream_id][0].append(curr_title)
                self.real_music[stream_id][1] = data
                result = data
                is_new = True
            else:
                result = None
                is_new = False
        else:
            if self.real_check_same(curr_title, stream_id):
                result = self.real_music[stream_id][1]
                is_new = False
            else:
                self.real_music[stream_id][0].append(curr_title)
                self.real_music[stream_id][1] = data
                result = data
                is_new = True

        if is_new and result:
            result = self.real_update_timestamp_utc(result, 'music')

        return result, is_new

    def deal_delay_music2(self, data):
        return self.deal_delay_music(data)

    def deal_delay_history(self, data):
        return self.deal_delay_music(data)

    def deal_delay_music(self, data):
        stream_id = data.get('stream_id')
        timestamp = data.get('timestamp')
        title_list = self.result_brief(data, 'music', self.delay_music_check_size)
        if stream_id not in self.delay_music:
            self.delay_music[stream_id] = [(title_list, timestamp, data)]
        else:
            self.delay_music[stream_id].append((title_list, timestamp, data))

        if len(self.delay_music[stream_id]) > self.delay_list_max_num :
            return self.run_delay_music(stream_id)
        else:
            return None


