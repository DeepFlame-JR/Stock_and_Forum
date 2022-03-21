import sys, os, platform, time
if 'Windows' not in platform.platform():
    os.environ['TZ'] = 'Asia/Seoul'
    time.tzset()

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(__file__))
from util import database, common
from word import word

import pyspark.sql.functions as f
from pyspark.sql import Row
from pyspark.sql import SQLContext
import spark_job
import datetime

def test(row):
    row_dict = row.asDict()
    row_dict['tt'] = row_dict['title'] + "_test"
    newRow = Row(**row_dict)
    return newRow

if __name__ == '__main__':
    Log = common.Logger(__file__)
    spark_counter = common.TimeCounter('Spark Operation')

    s = spark_job.SparkJob()

    # 일자 설정
    # date = datetime.date.today()
    date = datetime.date(2022,3,18)
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
                .filter(f.col('name') == '디어유')

    Log.info("Stock data count: " + str(stock_df.count()))
    Log.info("Forum data count: " + str(forum_df.count()))

    forum_df.show()
    w = word()

    rdd = forum_df.select('title').rdd
    
    # test 함수 실험
    tt = rdd.map(lambda x: test(x))
    print(type(tt))
    df = tt.toDF()
    df.show()

    # 같은 함수에 대해서 에러가 나타남 (cannot pickle '_jpype._JField' object)
    title_phrases1 = rdd.map(lambda row: w.test(row))
    print(type(title_phrases1))
    df1 = title_phrases1.toDF()
    df1.show()
    # title_phrases = rdd.map(lambda x: w.get_phrases_row(x, 'title')).collect()

    agg_df = forum_df.groupby('code', 'date').agg(
                f.count('_id').alias('forum_count'),
                f.sum('view').alias('forum_view'),
                f.sum('like').alias('forum_like'),
                f.sum('unlike').alias('forum_unlike'),
                f.avg(f.length('title')).alias('forum_title_lengthAvg'),
                f.avg(f.length('content')).alias('forum_content_lengthAvg')
    )

    result = stock_df.join(agg_df, ['code', 'date'], 'left')
    result.show(5)

    spark_counter.end()




