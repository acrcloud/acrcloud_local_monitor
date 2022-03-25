#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys
import MySQLdb
import traceback

reload(sys)
sys.setdefaultencoding("utf8")

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
