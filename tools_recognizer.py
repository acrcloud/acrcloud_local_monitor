#!/usr/bin/env python
#-*- coding:utf-8 -*-

import os
import sys
import hmac
import json
import time
import struct
import base64
import hashlib
import urllib2
import datetime
import mimetools
import traceback
import acrcloud_stream_tool as acrcloud_stream_decode

class acrcloud_recognize:

    def __init__(self, dlog):
        self.dlog = dlog
        self.noResult_count = 0
        self.rate = 0

    def post_multipart(self, url, fields, files, timeout):
        content_type, body = self.encode_multipart_formdata(fields, files)
        if not content_type and not body:
            self.dlog.logger.error('encode_multipart_formdata error')
            return None
        try:
            req = urllib2.Request(url, data=body)
            req.add_header('Content-Type', content_type)
            req.add_header('Referer', url)
            resp = urllib2.urlopen(req, timeout=timeout)
            ares = resp.read()
            return ares
        except Exception, e:
            self.dlog.logger.error('post_multipart error', exc_info=True)
        return None

    def encode_multipart_formdata(self, fields, files):
        try:
            boundary = mimetools.choose_boundary()
            CRLF = '\r\n'
            L = []
            for (key, value) in fields.items():
                L.append('--' + boundary)
                L.append('Content-Disposition: form-data; name="%s"' % key)
                L.append('')
                L.append(str(value))
            for (key, value) in files.items():
                L.append('--' + boundary)
                L.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (key, key))
                L.append('Content-Type: application/octet-stream')
                L.append('')
                L.append(value)
            L.append('--' + boundary + '--')
            L.append('')
            body = CRLF.join(L)
            content_type = 'multipart/form-data; boundary=%s' % boundary
            return content_type, body
        except Exception, e:
            self.dlog.logger.error('encode_multipart_formdata error', exc_info=True)
        return None, None

    def add_wav_header(self, buf, audio_sample_rate):
        try:
            buf_size = len(buf)
            flag_riff = 'RIFF'
            file_size = buf_size + 44 - 8
            flag_wave = 'WAVE'
            flag_fmt = 'fmt '
            version = 0x10
            iformat = 1
            channel = 1
            SamplePerSec = audio_sample_rate #8000
            AvgBytesPerSec = audio_sample_rate*2 #16000
            blockalign = 2
            BitPerSample = 16
            flag_data = 'data'
            pcm_size = buf_size
            wav_header = struct.pack('4si4s4sihhiihh4si', flag_riff, file_size, flag_wave, flag_fmt, version, iformat,
                                     channel, SamplePerSec, AvgBytesPerSec, blockalign, BitPerSample, flag_data, pcm_size)
            wav_buf = wav_header + buf
            return wav_buf
        except Exception as e:
            self.dlog.logger.error('Error@add_wav_header', exc_info=True)
        return buf

    def pcm_to_aac(self, pcm_buf, pcm_sample_rate=8000, atype='aac', is_strip=False):
        try:
            opt = {
                'sample_rate': pcm_sample_rate,
                'channels': 1,
                'bit_rate': 16*1024,
                'type': atype
            }
            if is_strip:
                opt['is_strip'] = 1
            if acrcloud_stream_decode:
                encoder = acrcloud_stream_decode.Encoder(opt)
                encoder.write(pcm_buf)
                abuf = encoder.read_all()
                return abuf
            else:
                return pcm_buf
        except Exception as e:
            self.dlog.logger.error("Error@pcm_to_aac", exc_info=True)
        return ''

    def gen_fp(self, buf, rate=0):
        return acrcloud_stream_decode.create_fingerprint(buf, False, 300, 0.3)

    def do_recogize(self, host, query_data, query_type, stream_id, access_key, access_secret, timeout=10):
        http_method = "POST"
        http_url_file = "/v1/monitor/identify"
        data_type = query_type
        signature_version = "1"
        timestamp = int(time.mktime(datetime.datetime.utcfromtimestamp(time.time()).timetuple()))
        sample_bytes = str(len(query_data))

        string_to_sign = http_method+"\n"+http_url_file+"\n"+access_key+"\n"+data_type+"\n"+signature_version+"\n"+str(timestamp)
        sign = base64.b64encode(hmac.new(str(access_secret), str(string_to_sign), digestmod=hashlib.sha1).digest())

        fields = {'access_key':access_key,
                  'stream_id':stream_id,
                  'sample_bytes':sample_bytes,
                  'timestamp':str(timestamp),
                  'signature':sign,
                  'data_type':data_type,
                  "signature_version":signature_version}

        server_url = 'http://' + host + http_url_file
        res = self.post_multipart(server_url, fields, {"sample" : query_data}, timeout)
        return res

    def recognize(self, host, wav_buf, query_type, stream_id, access_key, access_secret):
        try:
            res = ''
            fp_buf = ''
            fp_buf = self.gen_fp(wav_buf, self.rate)
            res = self.do_recogize(host, fp_buf, query_type, stream_id, access_key, access_secret)
        except Exception as e:
            self.dlog.logger.error('recognize error', exc_info=True)
        return res, ''

    def recognize_new(self, host, wav_buf, query_type, stream_id, access_key, access_secret, encode=False):
        try:
            res, in_buf = '', ''
            if query_type == 'audio':
                if not encode:
                    in_buf = self.add_wav_header(wav_buf, 8000)
                else:
                    in_buf = self.pcm_to_aac(wav_buf, 8000, 'aac', True)
            elif query_type == 'fingerprint':
                in_buf = self.gen_fp(wav_buf, self.rate)
            if in_buf:
                res = self.do_recogize(host, in_buf, query_type, stream_id, access_key, access_secret)
        except Exception as e:
            self.dlog.logger.error('recognize_by_fp error.stream_id:{0}'.format(stream_id), exc_info=True)
        return res

