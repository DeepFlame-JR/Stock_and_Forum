import sys, os, platform, time
from builtins import enumerate

if 'Windows' not in platform.platform():
    os.environ['TZ'] = 'Asia/Seoul'
    time.tzset()

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(__file__))
from util import database, common
from word import word

import pyspark.sql.functions as f
from pyspark.sql import functions as F
import spark_job
import datetime

if __name__ == '__main__':
    Log = common.Logger(__file__)
    spark_counter = common.TimeCounter('Spark Operation')

    s = spark_job.SparkJob()

    forum_df = s.mongodb_read('forumdb', 'naverforum') \
                .withColumn('datetime2', f.col('datetime') - f.expr('interval 9 hours'))\
                .drop('datetime')\
                .withColumnRenamed('datetime2', 'datetime')

    duplicate = forum_df.groupby('datetime', 'title', 'content', 'id').agg(
                F.count('_id').alias('forum_count')
                ) \
                .filter(f.col('forum_count') > 1)
    mongo = database.MongoDB()

    print(forum_df.count())
    print(duplicate.count()//2)
    for i, row in enumerate(duplicate.collect()):
        condition = {
            'datetime':row['datetime'],
            'title':row['title'],
            'content':row['content'],
            'id':row['id'],
        }
        du = mongo.find_item_one(condition=condition, db_name='forumdb', collection_name='naverforum')
        mongo.delete_item_one(condition=du, db_name='forumdb', collection_name='naverforum')

    forum_df = s.mongodb_read('forumdb', 'naverforum') \
                .withColumn('datetime2', f.col('datetime') - f.expr('interval 9 hours'))\
                .drop('datetime')\
                .withColumnRenamed('datetime2', 'datetime')
    print(forum_df.count())

