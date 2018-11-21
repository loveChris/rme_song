# -*- coding: UTF-8 -*-
# Author:Christina.Li
import sys

reload(sys)
sys.setdefaultencoding("utf8")

from get_data import get_data
import pandas as pd
import time
import csv




def to_csv(sql,filename,cur,title):

    cur.execute(sql)
    data = cur.fetchone()
    with open(filename,'wb') as obj:
        csv_writer = csv.writer(obj,dialect='excel')
        csv_writer.writerow(title)
        while data:
            csv_writer.writerow(data)
            data = cur.fetchone()

def get_df(filename):
    reader1 = pd.read_csv(filename, iterator=True)
    loop = True
    chunksize = 100000
    chunks = []
    while loop:
        try:
            chunk = reader1.get_chunk(chunksize)
            chunks.append(chunk)
        except StopIteration:
            loop = False
            pass
    df = pd.concat(chunks, ignore_index=True)
    return df

def df_insert(df1,df2):
    try:
        df_i = pd.merge(df1, df2, on=['itemid', 'weight_rank'], how='left')
        df_insert = df_i[df_i['descid_y'].isna()]
        df_insert = df_insert.drop(columns=["descid_y", "weight_y",
                                    "nodeid_y", "file_date_y","create_date","modify_date"])
        df_insert.columns = ['itemid', 'descid', 'weight', 'weight_rank', 'nodeid', 'file_date']
        df_insert["create_date"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        df_insert["modify_date"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        print "Insert: "+str(df_insert.shape[0])
        df_insert.to_csv("insert.csv",index=False,sep="\t")
    except Exception,e:
        print e

def df_update(df1,df2):
    try:
        df_u = pd.merge(df1,df2, on=['itemid', 'weight_rank'],how='inner')
        df_u1 = df_u[df_u["weight_x"] != df_u['weight_y']]
        df_u2 = df_u[df_u["descid_x"] != df_u['descid_y']]
        df_u3 = df_u[df_u["nodeid_x"] != df_u['nodeid_y']]
        df_uu = df_u1.append(df_u2)
        df_uu = df_uu.append(df_u3)
        df_uu = df_uu.drop_duplicates()
        df_uu = df_uu.drop(["descid_y", "weight_y", "nodeid_y", "file_date_y",'modify_date'], axis=1)
        df_uu.columns = ['itemid', 'descid', 'weight', 'weight_rank', 'nodeid', 'file_date','create_date']
        df_uu["modify_date"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())+":Update "+str(df_uu.shape[0])
        df_uu.to_csv("update.csv",index=False,sep="\t")
        return df_uu
    except Exception,e:
        print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()) + str(e)

def df_del(df1,df2):
    try:
        df_d = pd.merge(df1, df2, on=['itemid', 'weight_rank'], how='outer')
        df_d = df_d[df_d["descid_x"].isna()]
        df_d = df_d[df_d.itemid.isin(df1["itemid"])]
        # df_d = df_d.drop(columns=["descid_x", "weight_x", "nodeid_x", "file_date_x"])
        # df_d.columns = ['itemid', 'weight_rank', 'descid', 'weight', 'nodeid', 'file_date']
        print "Delete: "+str(df_d.shape[0])
        return df_d
    except Exception,e:
        print e

def update_mysql(numa,num,df_update,dbcode):
    db = get_data(dbcode)
    conn = db.conn
    cur = db.cur
    for i in range(numa,num):
        data = df_update.iloc[i]
        sql = """
        update music_lib.rme_song_mood
        set descid=%s,
        weight=%s,
        nodeid=%s,
        file_date='%s',
        modify_date='%s'
        where itemid=%s
        and weight_rank=%s
        """ % (data['descid'], data['weight'], data['nodeid'],
                           data['file_date'], time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                           data['itemid'], data['weight_rank'])
        cur.execute(sql)
    conn.commit()
    db.close_cnn()

def get_maxid(tablename,dbcode):
    db = get_data(dbcode)
    cur = db.cur
    sql = """
    select max(itemid) from %s
    """ % tablename
    cur.execute(sql)
    maxid = cur.fetchone()
    db.close_cnn()
    return maxid[0]

def getcount(dbcode,tablename):
    db = get_data(dbcode)
    cur = db.cur
    tablename = "gracenote.song_%s" % tablename
    sql = """
    select count(*) from %s
    """ % tablename
    cur.execute(sql)
    count = cur.fetchone()
    db.close_cnn()
    return count[0]
def loaddata(tablename):
    insersql = """
            load data local infile 'update.csv'
            into table %s
            IGNORE 1 LINES
            """ % tablename
    return insersql

if __name__ == '__main__':
    # # filename, startitemid, enditemid, dbcode
    # song_table = "gracenote.song_era"
    # rme_table = "music_lib.rme_song_era"
    songfile = "song.csv"
    rmefile = "rme.csv"
    # startitemid=1
    # enditemid=100
    # dbcode="DB2"
    # to_csv(song_table,songfile,startitemid,enditemid,dbcode)
    # to_csv(rme_table,rmefile,startitemid,enditemid,dbcode)
    df1 = get_df(songfile)
    df2 = get_df(rmefile)
    df_u = df_update(df1,df2)
    print df_u


