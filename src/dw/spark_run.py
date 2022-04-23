import sys, os, platform, time
if 'Windows' not in platform.platform():
    os.environ['TZ'] = 'Asia/Seoul'
    time.tzset()

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(__file__))
from util import database, common
from hive_job import HiveJob
from spark_job import SparkJob

import pyspark.sql.functions as f
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import datetime
import pandas as pd

def ETL(date):
    Log = common.Logger(__file__)
    spark_counter = common.TimeCounter('Spark Operation')
    try:
        s = SparkJob()
        h = HiveJob()

        # retires 대비
        today_df = h.Read("select * from stockdb.kosdaq where created='%s'" % date)
        if len(today_df) != 0:
            raise 'data is already inserted'

        # get data
        stock_df = s.postgresql_query("select * from kosdaq where date = '%s'" % date)
        if stock_df.count() == 0:
            raise Exception('today is not the opening date')
        Log.info("Stock data count: " + str(stock_df.count()))

        forum_df = s.mongodb_read_date('forumdb', 'naverforum', date)\
                    .withColumn('date', f.to_date(f.col('datetime')))
        Log.info("Forum data count: " + str(forum_df.count()))

        # 형태소 분해 (Okt)
        # forum_RDD = forum_df.rdd.map(lambda row: word.get_phrases_row(row, 'title'))

        agg_df = forum_df.groupby('code', 'date').agg(
            f.count('_id').alias('forum_count'),
            f.sum('view').alias('forum_view'),
            f.sum('like').alias('forum_like'),
            f.sum('unlike').alias('forum_unlike'),
            f.avg(f.length('title')).alias('forum_title_length_avg'),
            f.avg(f.length('content')).alias('forum_content_length_avg'),
            f.sum('reply_count').alias('forum_reply_count')
        )

        spark_df = stock_df.join(agg_df, ['code', 'date'], 'left')
        spark_df.show(5)

        # Hive 삽입
        today_kosdaq_df = spark_df.toPandas()
        today_kosdaq_df['year'] = date.year
        today_kosdaq_df['month'] = date.month
        today_kosdaq_df['day'] = date.day
        today_kosdaq_df.rename(columns= {'date':'created'})
        Log.info("Hive Insert")
        h.Insert(today_kosdaq_df, 'stockdb', 'kosdaq')
    except Exception as e:
        Log.error(e)
    finally:
        spark_counter.end()

if __name__ == '__main__':
    ETL(datetime.date.today())


