#!/usr/bin/env python
#-*- coding:utf -*-

import os
import traceback
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
#Establishing the connection

class PsqlManager:

    def __init__(self, host, port, user, passwd, dbname):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.dbname = dbname
        self.connect()

    def connect(self):
        self.conn = psycopg2.connect(database=self.dbname, user=self.user, password=self.passwd, host=self.host, port=self.port)
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.curs = self.conn.cursor()

    def commit(self):
        try:
            self.conn.commit();
        except Exception as e:
            self.connect()
            try:
                self.conn.commit();
            except Exception as e:
                raise

    def execute(self, sql, params=None):
        if params:
            try:
                self.curs.execute(sql, params)
            except Exception as e:
                self.connect()
                try:
                    self.curs.execute(sql, params)
                except Exception as e:
                    raise
        else:
            try:
                self.curs.execute(sql)
            except Exception as e:
                self.connect()
                try:
                    self.curs.execute(sql)
                except Exception as e:
                    raise
        return self.curs

    def executemany(self, sql, params):
        if params:
            try:
                self.curs.executemany(sql, params)
            except Exception as e:
                self.connect()
                try:
                    self.curs.executemany(sql, params)
                except Exception as e:
                    raise
        return self.curs

