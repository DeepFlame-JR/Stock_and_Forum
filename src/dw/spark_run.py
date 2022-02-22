import json
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from util import database, common

import pyspark.sql.functions as f
from pyspark.sql import functions as F
import spark_job
import datetime


if __name__ == '__main__':
    spark_counter = common.TimeCounter('Spark Operation')

    s = spark_job.SparkJob()

    # 일자 설정
    date = datetime.date.today()
    # date = datetime.date(2022,2,18)
    today, yesterday = date, date + datetime.timedelta(days=-1)
    start_datetime, end_datetime = datetime.datetime.combine(today, datetime.time(8,0,0)), datetime.datetime.combine(today, datetime.time(15,30,0))
    start_datetime, end_datetime = start_datetime + datetime.timedelta(hours=9), end_datetime + datetime.timedelta(hours=9) # Mongo DB가 UTC로 설정

    stock_df = s.postgresql_query("select * from kosdaq where date = '%s'" % date)
    forum_df = s.mongodb_read('forumdb', 'naverforum') \
                .filter(f.col('datetime').between(str(start_datetime), str(end_datetime)))\
                .withColumn('datetime2', f.col('datetime') - f.expr('interval 9 hours'))\
                .drop('datetime')\
                .withColumnRenamed('datetime2','datetime')

    forum_df.select('datetime').show()

    # t = forum_df.select('like').rdd.map(lambda x: x[0]+1).toDF(schema='integer')
    # t = forum_df.select('datetime').rdd.map(lambda x: datetime.datetime(x) + datetime.timedelta(hours=-9)).toDF(schema='timestamp')
    # t.show()

    # forum_df.select('like2').show()
    # forum_df.select('datetime').distinct().show()

    # agg_df = forum_df.groupby('code')\
    #         .agg(
    #             F.count('_id').alias('forum_count'),
    #             F.sum('view').alias('forum_view'),
    #             F.sum('like').alias('forum_like'),
    #             F.sum('unlike').alias('forum_unlike'),
    #             F.avg(F.length('title')).alias('forum_title_lengthAvg')
    #             )
    #
    # result = stock_df.join(agg_df, 'code', 'left')
    spark_counter.end()

    # print(result.count())
    # result.printSchema()
    # result.show()



