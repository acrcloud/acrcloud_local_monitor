#!/usr/bin/env python
#-*- coding:utf-8 -*-
"""
author: hong
Copyright (c) 2011 Adam Cohen
......

"""
import re
from fuzzywuzzy import fuzz
import sys

reload(sys)
sys.setdefaultencoding("utf8")

RE_SPECIAL_STRING = """[ \[\]［］\(\)（）\n\t\r,\.\:"'‘“<>《》!！?？&]"""
RE_SUB_STRING = "(\(.*\))|(\[.*\])|(（.*）)"
THREADHOLD = 75

def str_filter_sub(old_str):
    old_str_sub = re.sub(RE_SUB_STRING, "", old_str)
    new_str = re.sub(RE_SPECIAL_STRING, '', old_str_sub)
    return new_str

def str_filter(old_str):
    return re.sub(RE_SPECIAL_STRING, '', old_str).strip()

def str_sub(old_str):
	return re.sub(RE_SUB_STRING, "", old_str).strip()

def str_sim(str1_old, str2_old):
	'''
	warning: do not str1=str(str1)
	'''
	str1 = str(str1_old)
	str2 = str(str2_old)

	format_str1 = str_filter(str1.lower().strip())
	format_str2 = str_filter(str2.lower().strip())
	if format_str1 == format_str2 \
		or format_str1.find(format_str2) != -1 \
		or format_str2.find(format_str1) != -1:
		return True, ""

	format_str1 = str_filter_sub(str1.lower().strip())
	format_str2 = str_filter_sub(str2.lower().strip())
	ratio = fuzz.ratio(format_str1, format_str2)
	return ratio >= THREADHOLD \
			or format_str1 == format_str2 \
			or format_str1.find(format_str2) != -1 \
			or format_str2.find(format_str1) != -1 , str(ratio)
