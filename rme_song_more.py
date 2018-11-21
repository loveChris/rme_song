# -*- coding: UTF-8 -*-
# Author:Christina.Li
import sys

reload(sys)
sys.setdefaultencoding("utf8")

from public_fuc import *
from get_data import get_data
import time,csv
from log import Logger
from gevent import monkey;monkey.patch_all()
import gevent


class rme_song_more:
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

    def __song_sql(self,startitemid,enditemid):
        sql = """
        select
        itemid, descid, weight, weight_rank, nodeid, file_date
        from gracenote.song_%s
        where itemid >= %d
        and itemid < %d
        """ % (self.table,startitemid,enditemid)
        title = ['itemid', 'descid', 'weight', 'weight_rank',
                 'nodeid', 'file_date']
        return sql, title

    def __rme_sql(self,startitemid,enditemid):
        sql = """
            select
            itemid, descid, weight, weight_rank, nodeid, file_date,
            create_date,modify_date
            from music_lib.rme_song_%s 
            where itemid >= %d
            and itemid < %d
            """ % (self.table,startitemid,enditemid)
        title = ['itemid', 'descid', 'weight', 'weight_rank',
                 'nodeid', 'file_date', 'create_date', 'modify_date']
        return sql, title

    def inset_data(self):
        self.log.logger.info(self.rme_tablename + " begin")
        self.log.logger.info(self.rme_tablename + " getmaxid")
        try:
            song_maxid = get_maxid(self.song_tablename,self.dbcode)
            rme_maxid = get_maxid(self.rme_tablename,self.dbcode)
            limit = 1000000
            if song_maxid >= rme_maxid:
                a, b = divmod(song_maxid, limit)
                for i in range(a):
                    startitemid = i * limit
                    enditemid = (i + 1) * limit
                    self.log.logger.info(str(i)+ " to csv")
                    song_sql, song_title = self.__song_sql(startitemid,enditemid)
                    rme_sql, rme_title = self.__rme_sql(startitemid,enditemid)
                    to_csv(song_sql, self.song_csv, self.cur,song_title)
                    to_csv(rme_sql, self.rme_csv, self.cur,rme_title)
                    df1 = get_df(self.song_csv)
                    df2 = get_df(self.rme_csv)
                    self.log.logger.info(str(i) + " Execute success")
                    self.log.logger.info(str(i) + " Insert")
                    df_insert(df1, df2)
                    insersql = """
                            load data local infile "insert.csv"
                            into table %s
                            IGNORE 1 LINES
                            """ % self.rme_tablename
                    self.cur.execute(insersql)
                    self.conn.commit()
                    self.log.logger.info(str(i) + " Update")
                    df_u = df_update(df1, df2)
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

                    self.log.logger.info(str(i) + " Delete")
                    df_d = df_del(df1, df2)
                    e = df_d.shape[0]
                    for i in range(e):
                        data = df_d.iloc[i]
                        sql = """
                            delete from music_lib.rme_song_mood
                            where itemid=%s
                            and weight_rank=%s
                            """ % (data['itemid'], data['weight_rank'])
                        self.cur.execute(sql)
                    self.conn.commit()

                startitemid = a * limit
                enditemid = song_maxid + 1
                song_sql, song_title = self.__song_sql(startitemid, enditemid)
                rme_sql, rme_title = self.__rme_sql(startitemid, enditemid)
                to_csv(song_sql, self.song_csv, self.cur, song_title)
                to_csv(rme_sql, self.rme_csv, self.cur, rme_title)
                df1 = get_df(self.song_csv)
                df2 = get_df(self.rme_csv)
                self.log.logger.info(self.rme_tablename + " The End Execute success")
                self.log.logger.info(" The End Insert")
                df_insert(df1, df2)
                insersql = """
                load data local infile "insert.csv"
                into table %s
                IGNORE 1 LINES
                """ % self.rme_tablename
                self.cur.execute(insersql)
                self.conn.commit()
                self.log.logger.info(" The End Update")
                df_u = df_update(df1, df2)
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
                self.log.logger.info(" The End Delete")
                df_d = df_del(df1, df2)
                e = df_d.shape[0]
                for i in range(e):
                    data = df_d.iloc[i]
                    sql = """
                    delete from music_lib.rme_song_mood
                    where itemid=%s
                    and weight_rank=%s
                    """ % (data['itemid'], data['weight_rank'])
                    self.cur.execute(sql)
                self.conn.commit()


            else:
                a, b = divmod(rme_maxid, limit)
                for i in range(a):
                    startitemid = i * limit
                    enditemid = (i + 1) * limit
                    self.log.logger.info(str(i) + " to csv")
                    song_sql, song_title = self.__song_sql(startitemid, enditemid)
                    rme_sql, rme_title = self.__rme_sql(startitemid, enditemid)
                    to_csv(song_sql, self.song_csv, self.cur, song_title)
                    to_csv(rme_sql, self.rme_csv, self.cur, rme_title)
                    df1 = get_df(self.song_csv)
                    df2 = get_df(self.rme_csv)
                    self.log.logger.info(str(i) + " Execute success")
                    self.log.logger.info(str(i) + " Insert")
                    df_insert(df1, df2)
                    insersql = """
                    load data local infile "insert.csv"
                    into table %s
                    IGNORE 1 LINES
                    """ % self.rme_tablename
                    self.cur.execute(insersql)
                    self.conn.commit()
                    self.log.logger.info(str(i) + " Update")
                    df_u = df_update(df1, df2)
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
                    self.log.logger.info(str(i) + " Delete")
                    df_d = df_del(df1, df2)
                    e = df_d.shape[0]
                    for i in range(e):
                        data = df_d.iloc[i]
                        sql = """
                            delete from music_lib.rme_song_mood
                            where itemid=%s
                            and weight_rank=%s
                            """ % (data['itemid'], data['weight_rank'])
                        self.cur.execute(sql)
                    self.conn.commit()

                startitemid = a * limit
                enditemid = song_maxid + 1
                song_sql, song_title = self.__song_sql(startitemid, enditemid)
                rme_sql, rme_title = self.__rme_sql(startitemid, enditemid)
                to_csv(song_sql, self.song_csv, self.cur, song_title)
                to_csv(rme_sql, self.rme_csv, self.cur, rme_title)
                df1 = get_df(self.song_csv)
                df2 = get_df(self.rme_csv)
                self.log.logger.info(self.rme_tablename + " The End Execute success")
                self.log.logger.info(" The End Insert")
                df_insert(df1, df2)
                insersql = """
                load data local infile "insert.csv"
                into table %s
                IGNORE 1 LINES
                """ % self.rme_tablename
                self.cur.execute(insersql)
                self.conn.commit()
                self.log.logger.info(" The End Update")
                df_u = df_update(df1, df2)
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
                self.log.logger.info(" The End Delete")
                df_d = df_del(df1, df2)
                e = df_d.shape[0]
                for i in range(e):
                    data = df_d.iloc[i]
                    sql = """
                    delete from music_lib.rme_song_mood
                    where itemid=%s
                    and weight_rank=%s
                    """ % (data['itemid'], data['weight_rank'])
                    self.cur.execute(sql)
                self.conn.commit()

            self.log.remove()


        except Exception,e:
            self.log.logger.error(e)
    def __del__(self):
        pass











