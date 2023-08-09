import re
import os
import sys
import ssl
import json
import time
import socket
import random
import datetime
import traceback
import requests
from urllib.parse import urlparse

from random import choice

try:
    from bs4 import BeautifulSoup
except ImportError:
    from BeautifulSoup import BeautifulSoup

USER_AGENTS = [
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11',
    'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12',
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6',
    'Mozilla/5.0 (Macintosh; U; Intel Mac OS X; en) AppleWebKit/419 (KHTML, like Gecko) Safari/419.3',
    'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.83 Safari/537.1',
    'Mozilla/5.0 (Windows NT 6.1; rv:14.0) Gecko/20100101 Firefox/14.0.1'
]

CONTENT_TYPE_MAP = {
    'audio/x-scpls': {'ext': ['pls'], 'code': 1, 'r_ext': 'pls'},
    'audio/mpeg': {'ext': ['mp1', 'mp2', 'mp3'], 'code': 0, 'r_ext': ''},
    'audio/aacp': {'ext': ['acc', 'mp3'], 'code':0, 'r_ext': ''},
    'audio/x-mpegurl': {'ext': ['m3u'], 'code': 1, 'r_ext' :'m3u'},
    'audio/x-mpeg': {'ext': ['mp3'], 'code': 0, 'r_ext': ''},
    'audio/aac': {'ext': ['acc'], 'code': 0, 'r_ext': ''},

    'video/x-ms-asf': {'ext': ['asx', 'asf'], 'code': 1, 'r_ext': 'asf'},
    'video/mp2t': {'ext': ['ts'], 'code': 0, 'r_ext': ''},
    'video/x-flv': {'ext': ['flv'], 'code': 0, 'r_ext': ''},
    'video/x-ms-wmx': {'ext': ['wmx'], 'code':1, 'r_ext': 'wmx'},

    'application/ogg': {'ext': ['ogx'], 'code': 0, 'r_ext': ''},
    'application/x-mpegurl': {'ext': ['m3u8'], 'code':0, 'r_ext': ''},
    'application/pls+xml': {'ext': ['pls'], 'code': 1, 'r_ext': 'pls'},
    'application/flv': {'ext': ['flv'], 'code':0, 'r_ext': ''},
    'application/vnd.apple.mpegurl': {'ext': ['m3u8'], 'code': 0, 'r_ext': ''},
    'application/xspf+xml': {'ext': ['xspf'], 'code': 1, 'r_ext': 'xspf'},
    'application/force-download': {'ext': ['m3u8'], 'code': 1, 'r_ext': 'm3u8'},
}

class Tools_Url:

    def __init__(self):
        self.ext_func_map = {
            'm3u': self.parseM3U,
            'm3u8': self.parseM3U8,
            'xspf': self.parseXSPF,
            'pls': self.parsePLS,
            'asx': self.parseASX,
            'wmx': self.parseWMX,
            'asf': self.parseASF,
            'nsv': self.parseNSV
        }

    def getPage(self, url, referer=None, timeout=20, read_size=256):
        try:
            res = ''
            if referer:
                headers = { 'Referer': referer }
                r = requests.get(url, headers=headers, stream=True)
            else:
                r = requests.get(url, stream=True)
            if read_size:
                for b in r.iter_content(chunk_size=int(read_size)):
                    res = b.decode()
                    break
        except Exception as e:
            traceback.print_exc()
        return res

    def getContentType(self, url, timeout=20):
        try:
            res = ''
            r = requests.get(url, timeout=timeout)
            if r and 'Content-Type' in r.headers:
                res = r.headers['Content-Type']
        except Exception as e:
            #traceback.print_exc()
            print ('getContentType.url:{0}, error: {1}'.format(url, str(e)))
        return res

    def parsePLS(self, url):
        plslist = []
        pageinfo = self.getPage(url)
        plslist = re.findall(r'(http.*[^\r\n\t ])', pageinfo)
        return sorted(plslist)

    def parseASX(self, url):
        asxlist = []
        pageinfo = self.getPage(url)
        try:
            soup = BeautifulSoup(pageinfo, 'html5lib') ##sudo pip install html5lib
        except Exception as e:
            print ('Error@parseASX(html5lib), error_info: {0}'.format(str(e)))
            try:
                soup = BeautifulSoup(pageinfo)
            except Exception as e:
                print ('Error@parseASX(no html5lib), error_info: {0}'.format(str(e)))
                return asxlist

        entrylist = soup.findAll('entry')
        for entry in entrylist:
            reflist = entry.findAll('ref')
            hreflist = [ ref.get('href') for ref in reflist if ref.get('href')]
            for href in hreflist:
                href_ext = self.get_ext(href)
                if href_ext != 'asx':
                    true_urls = self.do_analysis_url(href)
                    asxlist.extend(true_urls)
        return sorted(asxlist)

    def parseASF(self, url):
        if url.startswith('http://'):
            url = 'mmsh' + url[4:]
        return [url]

    def parseM3U(self, url):
        m3ulist = []
        try:
            pageinfo = self.getPage(url = url, read_size=1024*1024*0.5)
        except Exception as e:
            traceback.print_exc()
        m3ulist = re.findall(r'(http.*[^\r\n\t "])', pageinfo)
        return sorted(m3ulist)

    def parseM3U8(self, url):
        m3u8list = [url]
        try:
            if url.find('.m3u8') != -1:
                pageinfo = self.getPage(url = url, read_size=1024*1024*0.5)
                if pageinfo and pageinfo.find('#EXTM3U') == -1:
                    new_url_list = re.findall(r'(http.*[^\r\n\t "])', pageinfo)
                    m3u8list.extend(new_url_list)
        except Exception as e:
            traceback.print_exc()
        return m3u8list

    def parseWMX(self, url):
        wmxlist = []
        pageinfo = self.getPage(url)
        try:
            soup = BeautifulSoup(pageinfo, 'html5lib') ##sudo pip install html5lib
        except Exception as e:
            print ('Error@parseWMX(html5lib), error_info: {0}'.format(str(e)))
            try:
                soup = BeautifulSoup(pageinfo)
            except Exception as e:
                print ('Error@parseWMX(no_html5lib), error_info: {0}'.format(str(e)))
                return wmxlist
        entry = soup.find('entry')
        ref = entry.find('ref')
        href = ref.get('href')
        wmxlist.append(href)
        return sorted(wmxlist)

    def parseXSPF(self, url):
        #introduce: http://www.xspf.org/quickstart/
        xspflist = []
        pageinfo = self.getPage(url)
        xmldoc = minidom.parseString(pageinfo)
        tracklist = xmldoc.getElementsByTagName('track')
        for track in tracklist:
            loc = track.getElementsByTagName('location')[0]
            xspflist.append(loc.childNodes[0].data)
        return sorted(xspflist)

    def parseMMS(self, url):
        mmslist = []
        convert = ['mmst', 'mmsh', 'rtsp']
        mmslist = [ conv + url[3:] for conv in convert ]
        return sorted(mmslist)

    def parseNSV(self, url):
        nsvlist = []
        if url.endswith('.nsv'):
            nsvlist.append(url[:-4])
        else:
            nsvlist.append(url)
        return nsvlist

    def get_ext(self, url):
        path = urlparse(url).path
        ext = os.path.splitext(path)[1]
        if ext and ext.startswith('.'):
            ext = ext[1:]
        return ext

    def get_ip(self, url):
        try:
            ip = ''
            hostname = urlparse(url).netloc
            ip = socket.gethostbyname(hostname)
        except Exception as e:
            ip = hostname
        return ip

    def get_header(self, url, timeout=20):
        try:
            r = requests.get(url, timeout=timeout)
            return r.headers
        except Exception as e:
            print ('Get headers error: ', str(e))
        return None

    def deal_by_content_type(self, url, content_type):
        try:
            slist = []
            if content_type in CONTENT_TYPE_MAP:
                tmp_info = CONTENT_TYPE_MAP[content_type]
                if tmp_info['code'] == 1:
                    r_ext = tmp_info['r_ext']
                    if r_ext in self.ext_func_map:
                        slist = self.ext_func_map[r_ext](url)
        except Exception as e:
            traceback.print_exc()
        return slist

    def deal_by_ext(self, url, ext):
        try:
            slist = []
            if ext in self.ext_func_map:
                slist = self.ext_func_map[ext](url)
        except Exception as e:
            traceback.print_exc()
        return slist

    def get_redirect_url(self, url):
        try:
            r = requests.get(url, allow_redirects=False)
            redirect_url = r.headers.get('Location', url)
            return redirect_url
        except Exception as e:
            traceback.print_exc()
        return url

    def do_analysis_url(self, url):
        try:
            url = url.strip()
            true_url_list = []
            netloc = urlparse(url).netloc
            if netloc in ['www.youtube.com', 'youtu.be']:
                if netloc == 'youtu.be':
                    url = self.get_redirect_url(url)
                youtb_url = self.get_youtube_url(url)
                true_url_list.append(youtb_url)
            else:
                if url.startswith('mms://'):
                    true_url_list = self.parseMMS(url)
                elif url.endswith('.nsv'):
                    true_url_list = self.parseNSV(url)
                else:
                    ext = self.get_ext(url)
                    if not ext:
                        content_type = self.getContentType(url)
                        if content_type:
                            true_url_list = self.deal_by_content_type(url, content_type)
                        else:
                            if url.startswith('http://'):
                                tmp_p =  urlparse(url)
                                if tmp_p.netloc.find(':') != -1:
                                    try_mmsh = url.replace('http://', 'mmsh://')
                                    true_url_list = [url, try_mmsh]
                    else:
                        true_url_list = self.deal_by_ext(url, ext)

        except Exception as e:
            traceback.print_exc()

        if not true_url_list:
            true_url_list.append(url)
        return true_url_list

    def get_youtube_url(self, url):
        try:
            youtb_url = ''
            page_info = self.getPage(url)
            start_find_str = ';ytplayer.config ='
            start_index = page_info.find(start_find_str)
            if start_index != -1:
                start_index += len(start_find_str)
            end_find_str = ';ytplayer.load'
            end_index = page_info.find(end_find_str)
            if end_index != -1 and start_index != -1:
                config_info = page_info[start_index:end_index]
                config_json = json.loads(config_info)
                youtb_url = config_json['args']['hlsvp']
        except Exception as e:
            traceback.print_exc()
        return youtb_url if youtb_url else url

