#!/usr/bin/env python
#-*- coding:utf-8 -*-

import os
import sys
import hmac
import json
import time
import base64
import hashlib
import urllib2
import datetime
import mimetools
import traceback
import acrcloud_extr

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

    def gen_fp(self, buf, rate=0):
        return acrcloud_extr.create_fingerprint(buf, (rate, -1*rate))

    def do_recogize(self, host, query_data, query_type, access_key, access_secret, timeout=5):
        http_method = "POST"
        http_url_file = "/v1/identify"
        data_type = query_type
        signature_version = "1"
        timestamp = int(time.mktime(datetime.datetime.utcfromtimestamp(time.time()).timetuple()))
        sample_bytes = str(len(query_data))

        string_to_sign = http_method+"\n"+http_url_file+"\n"+access_key+"\n"+data_type+"\n"+signature_version+"\n"+str(timestamp)
        sign = base64.b64encode(hmac.new(str(access_secret), str(string_to_sign), digestmod=hashlib.sha1).digest())
    
        fields = {'access_key':access_key, 
                  'sample_bytes':sample_bytes, 
                  'timestamp':str(timestamp), 
                  'signature':sign, 
                  'data_type':data_type, 
                  "signature_version":signature_version}
        
        server_url = 'http://' + host + http_url_file
        res = self.post_multipart(server_url, fields, {"sample" : query_data}, timeout)
        return res

    def recognize(self, host, wav_buf, query_type, access_key, access_secret, timeout=5, isCheck=False):
        try:
            res = ''
            pcm_buf = self.gen_fp(wav_buf, self.rate)
            res = self.do_recogize(host, pcm_buf, query_type, access_key, access_secret, timeout)
            if isCheck:
                self.check(res, wav_buf)
        except Exception as e:
            self.dlog.logger.error('recognize error', exc_info=True)
        return res
        
    def check(self, res, wav_buf):
        try:
            j_res = json.loads(res)
            if 'response' in j_res and j_res['response']['status']['code'] == 0:
                self.noResult_count = 0
            elif 'status' in j_res and j_res['status']['code'] == 0:
                self.noResult_count = 0
            else:
                self.noResult_count += 1
            if self.noResult_count in [2, 5, 9, 15] or (self.noResult_count > 15 and self.noResult_count % 10 == 0):
                self.tran_rate(wav_buf)
        except Exception as e:
            self.dlog.logger.error('recognize check error:', exc_info=True)
        
    def tran_rate(self, buf):
        self.dlog.logger.warn('tran_rate...')
        wav_buf = buf[:80000]
        retry_list = [0]
        for i in range(1, 31, 3):
            retry_list.append(i*0.1)
            retry_list.append(i*-0.1)
            
        for i in retry_list:
            try:
                pcm_buf = self.gen_fp(wav_buf, i)
                res = self.do_recogize(self.host, pcm_buf, self.query_type, self.access_key, self.access_secret, self.timeout)
                if res:
                    j_res = json.loads(res)
                    if 'response' in j_res and j_res['response']['status']['code'] == 0:
                        self.rate = i
                        self.noResult_count = 0
                        self.dlog.logger.warn('recognize@Change Rate Success: {0}'.format(self.rate))
                        break
                    elif 'status' in j_res and j_res['status']['code'] == 0:
                        self.rate = i
                        self.noResult_count = 0
                        self.dlog.logger.warn('recognize@Change Rate Success: {0}'.format(self.rate))
                        break
            except Exception as e:
                self.dlog.logger.error('recognize tran_rate error:', exc_info=True)
                break
            
