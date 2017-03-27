#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Sat Jan  7 15:59:14 2017

@author: WXB

包含操作数据库等函数或类，直接运行程序则死循环查询数据及更新数据库
"""


from datetime import datetime, timedelta
import time
import mysql.connector
from okcoin_spot_api import OKCoinSpotMarket


# 获取数据库中的数据
def get_history(lastTime, period=100, frequency='1day', field='close', 
                marketType='okcoin_btc_cny'):
    """
    lastTime: utc_p8,以datetime.datetime(2017 01 01 12 00 00)格式输入
    frequency:'1min', '3min', '5min', '15min', '30min', '1hour', '2hour', 
              '4hour', '6hour', '12hour', '1day', '3day', '1week'
              
    field:utc, utc_p8, timestamp, open, high, low, close, volume
    
    marketType:okcoin_btc_cny,(okcoin_ltc_cny,huobi_btc_cny等未开发)
    """
    conn = mysql.connector.connect(user='root', password='',
                                   database='okcoin_btc_cny')
    cursor = conn.cursor()
    
    cursor.execute("SELECT close \
                    FROM day_1 \
                    WHERE utc_p8 <= 2017-03-13 \
                    ORDER BY utc_p8 DESC \
                    LIMIT 100")
    
    cursor.close() #cur.close() 关闭游标 
    conn.commit() #conn.commit()方法在提交事物，在向数据库插入一条数据时必须要有这个
                  #方法，否则数据不会被真正的插入
    conn.close() #conn.close()关闭数据库连接


# 更新所有类型数据库
def update_database():
    update_database_week_1()
    update_database_day_1()
    update_database_hour_1()
    update_database_min_15()
    

def update_database_day_1():
    okcoinRESTURL = 'www.okcoin.cn'
    BTC_Market = OKCoinSpotMarket(okcoinRESTURL,'btc_cny')
    conn = mysql.connector.connect(user='root', password='',
                                   database='okcoin_btc_cny')
    cursor = conn.cursor()
    
    """
    以下为更新日线数据
    
    在命令行中创建数据库并添加第一条数据的脚本如下：
    （第一条数据需要手动添加，暂不考虑加入这个功能）
    CREATE TABLE day_1(
    id BIGINT NOT NULL AUTO_INCREMENT,
    utc DATETIME NOT NULL,
    utc_p8 DATETIME NOT NULL,
    timestamp BIGINT NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE NOT NULL,
    PRIMARY KEY(id,utc,utc_p8,timestamp)
    );

    日线第一条数据从2015年1月1日开始记录
    INSERT INTO day_1
    (utc,utc_p8,timestamp,open,high,low,close,volume)
    VALUES
    ('2014-12-31 16:00:00','2015-1-1 00:00:00',1420041600,1944.13,1974.0,1938.0,1950.0,53575.3741);
    """
    
    #取最后一条数据对比
    cursor.execute("SELECT * FROM day_1 ORDER BY id DESC LIMIT 1")
    last_data = cursor.fetchone()
    #print(last_data)

    #从网站上取数据
    klineType = '1day'
    klineSince = last_data[3]
    web_data = BTC_Market.kline(klineType,None,klineSince=klineSince*1000)

    #判断是否需要更新最近数据或补充数据
    if last_data[3]*1000 == web_data[-1][0]:
        #最后一条数据是现在这个时间段，则更新数据
        sqli = "UPDATE day_1 \
                SET open=%s,high=%s,low=%s,close=%s,volume=%s \
                WHERE timestamp=%s"
        cursor.execute(sqli,(web_data[-1][1],web_data[-1][2],web_data[-1][3],
                             web_data[-1][4],web_data[-1][5],
                             int(web_data[-1][0]/1000))) 
    else:
        #最后一条数据是很久之前的数据，需要先被更新，然后添加更新到最近的数据
        sqli = "UPDATE day_1 \
                SET open=%s,high=%s,low=%s,close=%s,volume=%s \
                WHERE timestamp=%s"
        cursor.execute(sqli,(web_data[0][1],web_data[0][2],web_data[0][3],
                             web_data[0][4],web_data[0][5],
                             int(web_data[0][0]/1000)))
        sqli = "INSERT INTO day_1 \
                (utc, utc_p8, timestamp, open, high, low, close, volume) \
                VALUES \
                (%s, %s, %s, %s, %s, %s, %s, %s)"
        for data in web_data[1:]:
            cursor.execute(sqli,(datetime.fromtimestamp(data[0]/1000)-timedelta(hours=8), 
                                 datetime.fromtimestamp(data[0]/1000), 
                                 int(data[0]/1000), data[1], data[2], data[3], 
                                 data[4], data[5]))


    cursor.close() #cur.close() 关闭游标 
    conn.commit() #conn.commit()方法在提交事物，在向数据库插入一条数据时必须要有这个方法，
              #否则数据不会被真正的插入
    conn.close() #conn.close()关闭数据库连接
    
              
def update_database_hour_1():
    okcoinRESTURL = 'www.okcoin.cn'
    BTC_Market = OKCoinSpotMarket(okcoinRESTURL,'btc_cny')
    conn = mysql.connector.connect(user='root', password='',
                                   database='okcoin_btc_cny')
    cursor = conn.cursor()
    
    """
    以下为更新小时线数据
    
    在命令行中创建数据库并添加第一条数据的脚本如下：
    （第一条数据需要手动添加，暂不考虑加入这个功能）
    CREATE TABLE hour_1(
    id BIGINT NOT NULL AUTO_INCREMENT,
    utc DATETIME NOT NULL,
    utc_p8 DATETIME NOT NULL,
    timestamp BIGINT NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE NOT NULL,
    PRIMARY KEY(id,utc,utc_p8,timestamp)
    );

    小时线数据从2016年11月1日0时开始记录，更早的数据网站已不提供
    INSERT INTO hour_1
    (utc,utc_p8,timestamp,open,high,low,close,volume)
    VALUES
    ('2016-10-31 16:00:00','2016-11-1 00:00:00',1477929600,4848.86,4861.0,4826.0,4851.74,348599.837);
    """
    
    #取最后一条数据对比
    cursor.execute("SELECT * FROM hour_1 ORDER BY id DESC LIMIT 1")
    last_data = cursor.fetchone()
    #print(last_data)

    #从网站上取数据
    klineType = '1hour'
    klineSince = last_data[3]
    web_data = BTC_Market.kline(klineType,None,klineSince=klineSince*1000)

    #判断是否需要更新最近数据或补充数据
    if last_data[3]*1000 == web_data[-1][0]:
        #最后一条数据是现在这个时间段，则更新数据
        sqli = "UPDATE hour_1 \
                SET open=%s,high=%s,low=%s,close=%s,volume=%s \
                WHERE timestamp=%s"
        cursor.execute(sqli,(web_data[-1][1],web_data[-1][2],web_data[-1][3],
                             web_data[-1][4],web_data[-1][5],
                             int(web_data[-1][0]/1000)))   
    else:
        #最后一条数据是很久之前的数据，需要先被更新，然后添加更新到最近的数据
        sqli = "UPDATE hour_1 \
                SET open=%s,high=%s,low=%s,close=%s,volume=%s \
                WHERE timestamp=%s"
        cursor.execute(sqli,(web_data[0][1],web_data[0][2],web_data[0][3],
                             web_data[0][4],web_data[0][5],
                             int(web_data[0][0]/1000)))
        sqli = "INSERT INTO hour_1 \
                (utc, utc_p8, timestamp, open, high, low, close, volume) \
                VALUES \
                (%s, %s, %s, %s, %s, %s, %s, %s)"
        for data in web_data[1:]:
            cursor.execute(sqli,(datetime.fromtimestamp(data[0]/1000)-timedelta(hours=8), 
                                 datetime.fromtimestamp(data[0]/1000), 
                                 int(data[0]/1000), data[1], data[2], data[3], 
                                 data[4], data[5]))


    cursor.close() #cur.close() 关闭游标 
    conn.commit() #conn.commit()方法在提交事物，在向数据库插入一条数据时必须要有这个方法，
              #否则数据不会被真正的插入
    conn.close() #conn.close()关闭数据库连接


def update_database_week_1():
    okcoinRESTURL = 'www.okcoin.cn'
    BTC_Market = OKCoinSpotMarket(okcoinRESTURL,'btc_cny')
    conn = mysql.connector.connect(user='root', password='',
                                   database='okcoin_btc_cny')
    cursor = conn.cursor()
    
    """
    以下为更新周线数据
    
    在命令行中创建数据库并添加第一条数据的脚本如下：
    （第一条数据需要手动添加，暂不考虑加入这个功能）
    CREATE TABLE week_1(
    id BIGINT NOT NULL AUTO_INCREMENT,
    utc DATETIME NOT NULL,
    utc_p8 DATETIME NOT NULL,
    timestamp BIGINT NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE NOT NULL,
    PRIMARY KEY(id,utc,utc_p8,timestamp)
    );

    周线数据从2013年7月1日0时开始记录
    INSERT INTO week_1
    (utc,utc_p8,timestamp,open,high,low,close,volume)
    VALUES
    ('2013-06-30 16:00:00','2013-07-1 00:00:00',1372608000,593.07,593.39,400.88,421.05,4042.27934448);
    """
    
    #取最后一条数据对比
    cursor.execute("SELECT * FROM week_1 ORDER BY id DESC LIMIT 1")
    last_data = cursor.fetchone()
    #print(last_data)

    #从网站上取数据
    klineType = '1week'
    klineSince = last_data[3]
    web_data = BTC_Market.kline(klineType,None,klineSince=klineSince*1000)

    #判断是否需要更新最近数据或补充数据
    if last_data[3]*1000 == web_data[-1][0]:
        #最后一条数据是现在这个时间段，则更新数据
        sqli = "UPDATE week_1 \
                SET open=%s,high=%s,low=%s,close=%s,volume=%s \
                WHERE timestamp=%s"
        cursor.execute(sqli,(web_data[-1][1],web_data[-1][2],web_data[-1][3],
                             web_data[-1][4],web_data[-1][5],
                             int(web_data[-1][0]/1000)))
    else:
        #最后一条数据是很久之前的数据，需要先被更新，然后添加更新到最近的数据
        sqli = "UPDATE week_1 \
                SET open=%s,high=%s,low=%s,close=%s,volume=%s \
                WHERE timestamp=%s"
        cursor.execute(sqli,(web_data[0][1],web_data[0][2],web_data[0][3],
                             web_data[0][4],web_data[0][5],
                             int(web_data[0][0]/1000)))
        sqli = "INSERT INTO week_1 \
                (utc, utc_p8, timestamp, open, high, low, close, volume) \
                VALUES \
                (%s, %s, %s, %s, %s, %s, %s, %s)"
        for data in web_data[1:]:
            cursor.execute(sqli,(datetime.fromtimestamp(data[0]/1000)-timedelta(hours=8), 
                                 datetime.fromtimestamp(data[0]/1000), 
                                 int(data[0]/1000), data[1], data[2], data[3], 
                                 data[4], data[5]))


    cursor.close() #cur.close() 关闭游标 
    conn.commit() #conn.commit()方法在提交事物，在向数据库插入一条数据时必须要有这个方法，
              #否则数据不会被真正的插入
    conn.close() #conn.close()关闭数据库连接   
   
    
def update_database_min_15():
    okcoinRESTURL = 'www.okcoin.cn'
    BTC_Market = OKCoinSpotMarket(okcoinRESTURL,'btc_cny')
    conn = mysql.connector.connect(user='root', password='',
                                   database='okcoin_btc_cny')
    cursor = conn.cursor()
    
    """
    以下为更新15分钟线数据
    
    在命令行中创建数据库并添加第一条数据的脚本如下：
    （第一条数据需要手动添加，暂不考虑加入这个功能）
    CREATE TABLE min_15(
    id BIGINT NOT NULL AUTO_INCREMENT,
    utc DATETIME NOT NULL,
    utc_p8 DATETIME NOT NULL,
    timestamp BIGINT NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE NOT NULL,
    PRIMARY KEY(id,utc,utc_p8,timestamp)
    );

    15分钟数据从2016年12月22日0时开始记录
    INSERT INTO min_15
    (utc,utc_p8,timestamp,open,high,low,close,volume)
    VALUES
    ('2016-12-21 16:00:00','2016-12-22 00:00:00',1482336000,5864.14,5915.0,5856.65,5899.29,66548.047);
    """
    
    #取最后一条数据对比
    cursor.execute("SELECT * FROM min_15 ORDER BY id DESC LIMIT 1")
    last_data = cursor.fetchone()
    #print(last_data)

    #从网站上取数据
    klineType = '15min'
    klineSince = last_data[3]
    web_data = BTC_Market.kline(klineType,None,klineSince=klineSince*1000)

    #判断是否需要更新最近数据或补充数据
    if last_data[3]*1000 == web_data[-1][0]:
        #最后一条数据是现在这个时间段，则更新数据
        sqli = "UPDATE min_15 \
                SET open=%s,high=%s,low=%s,close=%s,volume=%s \
                WHERE timestamp=%s"
        cursor.execute(sqli,(web_data[-1][1],web_data[-1][2],web_data[-1][3],
                             web_data[-1][4],web_data[-1][5],
                             int(web_data[-1][0]/1000)))
    else:
        #最后一条数据是很久之前的数据，需要先被更新，然后添加更新到最近的数据
        sqli = "UPDATE min_15 \
                SET open=%s,high=%s,low=%s,close=%s,volume=%s \
                WHERE timestamp=%s"
        cursor.execute(sqli,(web_data[0][1],web_data[0][2],web_data[0][3],
                             web_data[0][4],web_data[0][5],
                             int(web_data[0][0]/1000)))
        sqli = "INSERT INTO min_15 \
                (utc, utc_p8, timestamp, open, high, low, close, volume) \
                VALUES \
                (%s, %s, %s, %s, %s, %s, %s, %s)"
        for data in web_data[1:]:
            cursor.execute(sqli,(datetime.fromtimestamp(data[0]/1000)-timedelta(hours=8), 
                                 datetime.fromtimestamp(data[0]/1000), 
                                 int(data[0]/1000), data[1], data[2], data[3], 
                                 data[4], data[5]))


    cursor.close() #cur.close() 关闭游标 
    conn.commit() #conn.commit()方法在提交事物，在向数据库插入一条数据时必须要有这个
                  #方法，否则数据不会被真正的插入
    conn.close() #conn.close()关闭数据库连接     
    
  
# 直接运行本模块则循环更新行情数据，直至退出            
def main():
    while True:
        time.sleep(3)
        update_database()
        print("Now : %s" % time.ctime()) #调试时知道这个程序在运行
        
if __name__ == "__main__":
    main()