import sys
import pymysql
import traceback

class MysqlManager:

    def __init__(self, host, port, user, passwd, dbname):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.dbname = dbname
        self.conn = pymysql.connect(host=host, user=user,
                                    passwd=passwd, db=dbname,
                                    port=port, charset='utf8')
        self.curs = self.conn.cursor()

    def reconnect(self):
        self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user,
                passwd=self.passwd, db=self.dbname, charset='utf8')
        self.curs = self.conn.cursor()

    def commit(self):
        try:
            self.conn.commit();
        except (AttributeError, pymysql.Error):
            self.reconnect()
            try:
                self.conn.commit();
            except pymysql.Error:
                raise

    def execute(self, sql, params=None):
        self.conn.ping(reconnect=True)
        if params:
            try:
                self.curs.execute(sql, params)
            except (AttributeError, pymysql.Error):
                self.reconnect()
                try:
                    self.curs.execute(sql, params)
                except pymysql.Error:
                    raise
        else:
            try:
                self.curs.execute(sql)
            except (AttributeError, pymysql.Error):
                self.reconnect()
                try:
                    self.curs.execute(sql)
                except pymysql.Error:
                    raise
        return self.curs

    def executemany(self, sql, params):
        if params:
            try:
                self.curs.executemany(sql, params)
            except (AttributeError, pymysql.Error):
                self.reconnect()
                try:
                    self.curs.executemany(sql, params)
                except pymysql.Error:
                    raise
        return self.curs

    def insert_results(self, params):
        try:
            self.conn.ping(reconnect=True)
            sql = '''insert into result_info (access_key, stream_url, stream_id, result, timestamp, catchDate) values (%s, %s, %s, %s, %s, %s) on duplicate key update id=LAST_INSERT_ID(id)'''
            self.curs.execute(sql, params)
            self.conn.commit()
        except Exception as e:
            traceback.print_exc()
