#!/usr/bin/env python
#-*- coding:utf-8 -*-

import os
import sys
import time
import psutil
import datetime
from dateutil.relativedelta import *

def kill_9(pid):
    cmd="kill -9 {0}".format(pid)
    os.system(cmd)

def kill_all_ffmpeg_video(parent_pid):
    current_process = psutil.Process(parent_pid)#(os.getpid())
    for child in current_process.children(recursive=True):
        cmd = child.cmdline()
        cpid = child.pid
        if cmd[0].endswith("python") and cmd[-1].endswith("acrcloud_local_server.py"):
            basename = os.path.basename(cmd[-1])
            print "Find to Kill. PID:{0}, CMD:{1}".format(cpid, cmd)
            kill_9(cpid)


if __name__ == "__main__":
    parent_pid = 1
    kill_all_ffmpeg_video(parent_pid)

