import os
import sys
import json
import queue
import random
import signal
import struct
import base64
import logging
import datetime
import traceback
from dateutil.relativedelta import *

sys.path.append("..")
from acrcloud_logger import AcrcloudLogger as recordLogger

class RecordWorker:

    def __init__(self, config, workQueue):
        self.config = config
        self.record_keep_days = config['record']['record_save_days']
        self.record_dir = config['record']['record_dir']
        self.logdir = config['log']['dir']
        self.workQueue = workQueue
        self.recordDict = {} # key: stream_id, value: [(timestamp, record_before, record_after, now_buf, before_buf), ...]
        self.record_max_len = 100
        self.initLog()
        self.test_record_dir()
        self.dlog.logger.warning('Warn@Acrcloud_Record_Worker Init Success!')
        #signal.signal(signal.SIGQUIT, self.signal_handler)

    def initLog(self):
        self.dlog = recordLogger('RecordLog', logging.INFO)
        if not self.dlog.addFilehandler(logfile = 'recordLog.lst', logdir = self.logdir):
            sys.exit(1)
        if not self.dlog.addStreamHandler():
            sys.exit(1)

    def signal_handler(self, signal, frame):
        self.dlog.logger.error('Receive signal.SIGQUIT, recordWorker exit')
        sys.exit(1)

    def test_record_dir(self):
        try:
            if not os.path.exists(self.record_dir):
                os.mkdir(self.record_dir)
        except Exception as e:
            self.dlog.logger.error('Error@Record_Worker.test_record_dir', exc_info=True)
            self.dlog.logger.error('Error@Record_Worker.record_worker will exit, please check record_dir')

    def check_expire_dir(self):
        try:
            if self.record_keep_days >0 and random.random() < 0.3:
                nowtime = datetime.datetime.utcnow()
                if nowtime.hour == 0 and nowtime.minute < 5:
                    self.auto_delete_overtime_records()
        except Exception as e:
            self.dlog.logger.error('Error@Record_Worker.check_expire_dir', exc_info=True)

    def auto_delete_overtime_records(self):
        try:
            self.dlog.logger.info('MSG@Record_Worker.auto_delete_overtime_records start...')
            expire_date_str = (datetime.datetime.utcnow() - relativedelta(days=self.record_keep_days)).strftime('%Y%m%d')
            stream_list = []
            for root, dirs, files in os.walk(self.record_dir):
                stream_list = dirs
                break
            for stream_id in stream_list:
                stream_record_dir = os.path.join(self.record_dir, stream_id)
                stream_datestr = []
                for root, dirs, files in os.walk(stream_record_dir):
                    stream_datestr = dirs
                    break
                for datestr in stream_datestr:
                    if datestr < expire_date_str:
                        stream_date_record_dir = os.path.join(stream_record_dir, datestr)
                        for sd_root, sd_dirs, sd_files in os.walk(stream_date_record_dir):
                            for sd_file in sd_files:
                                file_path = os.path.join(stream_date_record_dir, sd_file)
                                if os.path.isfile(file_path):
                                    os.remove(file_path)
                                    #print "delete file: ", file_path
                        os.rmdir(stream_date_record_dir)
                        self.dlog.logger.warning('Warn@Record_Worker.remove_expire_date_record_dir: {0} delete success'.format(stream_date_record_dir))
        except Exception as e:
            self.dlog.logger.error('Error@Record_Worker.auto_delete_overtime_records', exc_info=True)

    def save_record(self, streamId, acrId, timestamp, audio_file):
        try:
            fileName = timestamp + '_' + acrId + '.wav'
            dateStr = timestamp[:8]
            streamPath = os.path.join(self.record_dir, streamId)
            if not os.path.exists(streamPath):
                os.mkdir(streamPath)
            datePath = os.path.join(streamPath, dateStr)
            if not os.path.exists(datePath):
                os.mkdir(datePath)
            filePath = os.path.join(datePath, fileName)
            with open(filePath, 'wb') as wfile:
                wfile.write(audio_file)
            return True
        except Exception as e:
            self.dlog.logger.error('Error@Record_Worker.save_record', exc_info=True)
        return False

    def add_wav_header(self, buf):
        buf_size = len(buf)
        flag_riff = 'RIFF'
        file_size = buf_size + 44 - 8
        flag_wave = 'WAVE'
        flag_fmt = 'fmt '
        version = 0x10
        iformat = 1
        channel = 1
        SamplePerSec = 8000
        AvgBytesPerSec = 16000
        blockalign = 2
        BitPerSample = 16
        flag_data = 'data'
        pcm_size = buf_size
        wav_header = struct.pack('4si4s4sihhiihh4si', flag_riff, file_size, flag_wave, flag_fmt, version, iformat,
                                 channel, SamplePerSec, AvgBytesPerSec, blockalign, BitPerSample, flag_data, pcm_size)
        wav_buf = wav_header + buf
        return wav_buf

    def get_acrid(self, info):
        md5 = 'noResult'
        try:
            if info['status']['code'] == 0:
                if 'custom_files' in info['metadata'] and info['metadata']['custom_files']:
                    md5 = info['metadata']['custom_files'][0]['acrid']
                elif 'music' in info['metadata'] and info['metadata']['music']:
                    md5 = info['metadata']['music'][0]['acrid']
        except Exception as e:
            self.dlog.logger.error('Error@Record_Worker.get_acrid', exc_info=True)
        return md5

    def get_duration(self, info):
        duration = 0
        try:
            if info['status']['code'] == 0:
                if 'custom_files' in info['metadata'] and info['metadata']['custom_files']:
                    duration = int(info['metadata']['custom_files'][0]['duration_ms']) / 1000
                elif 'music' in info['metadata'] and info['metadata']['music']:
                    duration = int(info['metadata']['music'][0]['duration_ms']) / 1000
        except Exception as e:
            self.dlog.logger.error('Error@Record_Worker.get_duration.error_data: {0}'.format(info), exc_info=True)
        return duration

    def get_played_duration(self, info):
        played_duration = 0
        try:
            if info['status']['code'] == 0:
                if 'metadata' in info and 'played_duration' in info['metadata']:
                    played_duration = int(info['metadata']['played_duration'])
        except Exception as e:
            self.dlog.logger.error('Error@Record_Worker.get_duration.error_data: {0}'.format(info), exc_info=True)
        return played_duration

    def get_timestamp_utc(self, info):
        timestamp_utc = ''
        try:
            if info['status']['code'] == 0:
                if 'metadata' in info and 'timestamp_utc' in info['metadata']:
                    timestamp_utc = info['metadata']['timestamp_utc']
        except Exception as e:
            self.dlog.logger.error('Error@Record_Worker.get_timestamp_utc.error_data: {0}'.format(info), exc_info=True)
        return timestamp_utc

    def format_timestamp(self, timestr):
        return datetime.datetime.strptime(timestr, '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S')

    def deal_add(self, info):
        try:
            #timestamp, record_before, record_after, now_buf, before_buf
            stream_id = info.get('stream_id')
            record = info.get('record')
            pem_file = base64.b64decode(info.get('pem_file',''))
            timestamp = self.format_timestamp(info.get('timestamp'))
            if stream_id and record and record[0] >= 1:
                if stream_id not in self.recordDict:
                    self.recordDict[stream_id] = []
                record_before = record[1]
                before_buf = ''
                '''
                for item in self.recordDict[stream_id]:
                    before_buf = item[3] + before_buf
                    if len(before_buf) >= record_before*16000:
                        before_buf = before_buf[-record_before*16000:]
                        break
                '''
                #数据进行recordDict头插
                self.recordDict[stream_id].insert(0, (timestamp, record[1], record[2], pem_file, before_buf))
                #避免保存的数据过多
                if len(self.recordDict[stream_id]) > self.record_max_len:
                    self.recordDict[stream_id] = self.recordDict[stream_id][:self.record_max_len]
                if random.random() < 1:#0.33:
                    self.dlog.logger.info('INFO@Record.deal_add success {0}, {1}, pem:{2}, before:{3}'.format(stream_id, str(record), len(pem_file), len(before_buf)))
        except Exception as e:
            self.dlog.logger.error('Error@Record_Worker.deal_add', exc_info=True)

    def deal_save(self, info):
        try:
            stream_id = info.get('stream_id')
            timestamp = info.get('timestamp')
            timestamp_format = self.format_timestamp(info.get('timestamp'))
            timestamp_utc = self.get_timestamp_utc(info.get('result', {}))
            if not timestamp_utc:
                timestamp_utc = timestamp

            #相对偏差，准确的开始播放时间timestamp_utc相对于timestamp的前后偏差值
            #该值可为正负，正代表开始时间在timestamp之前，负代表开始时间在timestamp之后
            tformat = '%Y-%m-%d %H:%M:%S'
            relative_deviation = int((datetime.datetime.strptime(timestamp, tformat) - datetime.datetime.strptime(timestamp_utc, tformat)).total_seconds())

            if stream_id and timestamp_format:
                if stream_id in self.recordDict:
                    save_index = None
                    for index in range(len(self.recordDict[stream_id]))[::-1]:
                        if timestamp_format == self.recordDict[stream_id][index][0]:
                            save_index = index
                            break
                    if save_index is not None:
                        md5 = self.get_acrid(info.get('result', ''))
                        #duration = self.get_duration(info.get('result', ''))
                        played_duration = self.get_played_duration(info.get('result', ''))
                        if played_duration == 0:
                            played_duration = self.get_duration(info.get('result', ''))
                        #获取播放开始前的音频
                        record_before = self.recordDict[stream_id][save_index][1]
                        record_after = self.recordDict[stream_id][save_index][2]
                        total_cut_seconds = record_before + played_duration + record_after #截取音频的总长度
                        #截取分为两部分，first_part, second_part
                        first_part_seconds = record_before + relative_deviation
                        first_part_buf = ''
                        second_part_seconds = total_cut_seconds - first_part_seconds
                        second_part_buf = ''
                        if first_part_seconds >= 0:
                            for i in range(save_index+1, len(self.recordDict[stream_id])):
                                first_part_buf = self.recordDict[stream_id][i][3] + first_part_buf
                                if len(first_part_buf) >= first_part_seconds*16000:
                                    first_part_buf = first_part_buf[-first_part_seconds*16000:]
                                    break
                        else:
                            skip_count = 0
                            for i in range(save_index, -1, -1):
                                first_part_buf += self.recordDict[stream_id][i][3]
                                skip_count += 1
                                if len(first_part_buf) >= abs(first_part_seconds)*16000:
                                    first_part_buf = first_part_buf[first_part_seconds*16000:]
                                    break
                            first_part_seconds_new = int(len(first_part_buf)/16000.0)
                            second_part_seconds = total_cut_seconds - first_part_seconds_new
                            save_index -= skip_count

                        for i in range(save_index, -1, -1):
                            second_part_buf += self.recordDict[stream_id][i][3]
                            if len(second_part_buf) >= second_part_seconds*16000:
                                second_part_buf = second_part_buf[:second_part_seconds*16000]
                                break
                        save_buf = first_part_buf + second_part_buf
                        save_buf_final = self.add_wav_header(save_buf)
                        save_size = len(save_buf_final)
                        ret = self.save_record(stream_id, md5, timestamp_format, save_buf_final)
                        if ret:
                            self.dlog.logger.warn('Warning@Record.deal_save.Success ({0}, {1}, {2}, size:({3}))'.format(stream_id, md5, timestamp_format, save_size))
                        else:
                            self.dlog.logger.warn('Warning@Record.deal_save.Failed ({0}, {1}, {2})'.format(md5, timestamp_format, ret[1]))
                    else:
                        self.dlog.logger.warn('Warning@Record.deal_save.no_save_index ({0}, {1})'.format(stream_id, timestamp_format))
        except Exception as e:
            self.dlog.logger.error('Error@Record_Worker.deal_save', exc_info=True)

    def deal_deleted(self, info):
        try:
            stream_id = info['stream_id']
            if stream_id in self.recordDict:
                del self.recordDict[stream_id]
                self.dlog.logger.warn('Warn@Record_Worker.deal_deleted.SID:{0} Record_buf del success'.format(stream_id))
        except Exception as e:
            self.dlog.logger.error('Error@Record_Worker.deal_deleted', exc_info=True)

    def start(self):
        self.Runing = True
        while 1:
            if not self.Runing:
                break
            try:
                recinfo = self.workQueue.get()
            except queue.Empty:
                continue
            if recinfo[0] == 'add':
                self.deal_add(recinfo[1])
            elif recinfo[0] == 'save':
                self.deal_save(recinfo[1])
            elif recinfo[0] == 'deleted':
                self.deal_deleted(recinfo[1])
            #auto delete expire record files
            self.check_expire_dir()

    def stop(self):
        self.Runing = False

def recordWorker(config, recordQueue):
    rw = RecordWorker(config, recordQueue)
    rw.start()
