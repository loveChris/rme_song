# -*- coding: UTF-8 -*-
# Author:Christina.Li



import pandas as pd
from public_fuc import *
from get_data import get_data
import time,csv
from log import Logger



class rme_song_less:
    def __init__(self,dbcode,tablename):
        self.dbcode = dbcode
        self.db = get_data(dbcode)
        self.conn = self.db.conn
        self.cur = self.db.cur
        self.table = tablename
        self.rme_tablename = "music_lib.rme_song_%s" % tablename
        self.song_tablename = "gracenote.song_%s" % tablename
        self.song_csv = "song.csv"
        self.rme_csv = "rme.csv"
        self.log = Logger("log.log")

    def __song_sql(self):
        sql = """
        select
        itemid, descid, weight, weight_rank, nodeid, file_date
        from gracenote.song_%s
        """ % self.table
        title = ['itemid', 'descid', 'weight', 'weight_rank',
                 'nodeid', 'file_date']
        return sql, title

    def __rme_sql(self):
        sql = """
            select
            itemid, descid, weight, weight_rank, nodeid, file_date,
            create_date,modify_date
            from music_lib.rme_song_%s 
            """ % self.table
        title = ['itemid', 'descid', 'weight', 'weight_rank',
                 'nodeid', 'file_date', 'create_date', 'modify_date']
        return sql, title

    def to_csv(self,sql,filename,title):
        self.cur.execute(sql)
        data = self.cur.fetchone()
        with open(filename, 'wb') as obj:
            csv_writer = csv.writer(obj, dialect='excel')
            csv_writer.writerow(title)
            while data:
                csv_writer.writerow(data)
                data = self.cur.fetchone()



    def insert_data(self):
        self.log.logger.info(self.rme_tablename+" begin")
        self.log.logger.info(self.rme_tablename + " to csv")
        song_sql, song_title = self.__song_sql()
        rme_sql, rme_title = self.__rme_sql()
        self.to_csv(song_sql,self.song_csv,song_title)
        self.to_csv(rme_sql,self.rme_csv,rme_title)
        self.log.logger.info(self.rme_tablename + " get df")
        df1 = get_df(self.song_csv)
        df2 = get_df(self.rme_csv)
        self.log.logger.info("Execute success")
        self.log.logger.info("Insert")
        df_insert(df1,df2)
        insersql = """
        load data local infile "insert.csv"
        into table %s
        IGNORE 1 LINES
        """ % self.rme_tablename
        self.cur.execute(insersql)
        self.conn.commit()
        self.log.logger.info("Update")
        df_u = df_update(df1,df2)
        length = df_u.shape[0]
        for i in range(length):
            data = df_u.iloc[i]
            sql = """
                delete from %s
                where itemid=%s
                and weight_rank=%s
                """ % (self.rme_tablename, data['itemid'], data['weight_rank'])
            self.cur.execute(sql)
        self.conn.commit()
        sql = loaddata(self.rme_tablename)
        self.cur.execute(sql)
        self.conn.commit()
        self.log.logger.info("Delete")
        df_d = df_del(df1,df2)
        e = df_d.shape[0]
        for i in range(e):
            data = df_d.iloc[i]
            sql = """
            delete from %s
            where itemid=%s
            and weight_rank=%s
            """ % (self.rme_tablename,data['itemid'],data['weight_rank'])
            self.cur.execute(sql)
        self.conn.commit()

        self.log.logger.info(self.rme_tablename + " end")
        self.db.close_cnn()
        self.log.remove()

    def __del__(self):
        pass
if __name__ == '__main__':
    artisttype = rme_song_less(dbcode="DB2",tablename="era")
    artisttype.insert_data()



