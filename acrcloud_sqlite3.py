import os
import sys
import sqlite3
import traceback

class Sqlite3Manager:

    def __init__(self, db_fpath):
        self.db_fpath = db_fpath
        fdir = os.path.dirname(self.db_fpath)
        if not os.path.exists(fdir):
            os.makedirs(fdir)
        self.connect()

    def connect(self):
        self.conn = sqlite3.connect(self.db_fpath, check_same_thread=False)
        self.curs = self.conn.cursor()
        self.check_table()

    def commit(self):
        try:
            self.conn.commit();
        except Exception as e: 
            traceback.print_exc()

    def execute(self, sql, params=None):
        try:
            if params:
                self.curs.execute(sql, params)
            else:
                self.curs.execute(sql)
        except Exception as e:
            traceback.print_exc()
        return self.curs

    def check_table(self):
        try:
            sql_count = '''SELECT count(*) FROM sqlite_master WHERE type=? AND name=?;'''
            self.curs.execute(sql_count, ('table', 'result_info'))
            count = self.curs.fetchone()[0]
            print ('count: ', count)
            if count == 0:
                sql_table = '''
                    create table if not exists result_info (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    access_key TEXT,
                    stream_url TEXT,
                    stream_id TEXT,
                    result TEXT,
                    timestamp TEXT,
                    catchDate TEXT);
                '''
                self.curs.execute(sql_table)
                sql_index = '''CREATE INDEX sid ON result_info (stream_id);'''
                self.curs.execute(sql_index)
                print ('Create table monitoring_results')
        except Exception as e:
            traceback.print_exc()

    def insert_results(self, params):
        sql = '''insert into result_info(access_key, stream_url,stream_id, result, timestamp, catchDate) values(?,?,?,?,?,?)'''
        self.curs.execute(sql, params)
        self.conn.commit()

if __name__ == '__main__':
    db_fpath = './test.db' 
    sq = Sqlite3Manager(db_fpath)
