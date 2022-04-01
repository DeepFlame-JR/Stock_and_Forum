import sys, os, platform, time
if 'Windows' not in platform.platform():
    os.environ['TZ'] = 'Asia/Seoul'
    time.tzset()

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(__file__))
from util import database, common
import word

import pyspark.sql.functions as f
from pyspark.sql import Row
from pyspark.sql.types import *
import spark_job
import datetime

if __name__ == '__main__':
    Log = common.Logger(__file__)
    spark_counter = common.TimeCounter('Spark Operation')

    s = spark_job.SparkJob()

    # 일자 설정
    date = datetime.date.today()
    # date = datetime.date(2022,3,22)
    today, yesterday = date, date + datetime.timedelta(days=-1)
    start_datetime, end_datetime = datetime.datetime.combine(today, datetime.time(8,0,0)), datetime.datetime.combine(today, datetime.time(15,30,0))
    start_datetime, end_datetime = start_datetime + datetime.timedelta(hours=9), end_datetime + datetime.timedelta(hours=9) # Mongo DB가 UTC로 설정

    # get data
    stock_df = s.postgresql_query("select * from kosdaq where date = '%s'" % date)
    forum_df = s.mongodb_read('forumdb', 'naverforum') \
                .filter(f.col('datetime').between(str(start_datetime), str(end_datetime)))\
                .withColumn('datetime2', f.col('datetime') - f.expr('interval 9 hours'))\
                .drop('datetime')\
                .withColumnRenamed('datetime2', 'datetime')\
                .withColumn('date', f.to_date(f.col('datetime')))\

    Log.info("Stock data count: " + str(stock_df.count()))
    Log.info("Forum data count: " + str(forum_df.count()))

    # 형태소 분해 (Okt)
    # forum_RDD = forum_df.rdd.map(lambda row: word.get_phrases_row(row, 'title'))
    # print(type(forum_RDD))
    # print(forum_RDD.collect())

    agg_df = forum_df.groupby('code', 'date').agg(
                f.count('_id').alias('forum_count'),
                f.sum('view').alias('forum_view'),
                f.sum('like').alias('forum_like'),
                f.sum('unlike').alias('forum_unlike'),
                f.avg(f.length('title')).alias('forum_title_length_avg'),
                f.avg(f.length('content')).alias('forum_content_length_avg'),
                f.sum('reply_count').alias('forum_reply_count')
    )

    result = stock_df.join(agg_df, ['code', 'date'], 'left')

    # result.printSchema()
    # DF = result.todf()
    # print(type(DF))

    spark_counter.end()