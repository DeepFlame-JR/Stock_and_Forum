import sys, os, io, platform, time

import pandas as pd

if 'Windows' not in platform.platform():
    os.environ['TZ'] = 'Asia/Seoul'
    time.tzset()

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sys.path.append((os.path.dirname(__file__)))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from util import database, common

import datetime
from pyspark.sql import SparkSession

class SparkJob(object):
    def __init__(self):
        self.config = common.Config()
        self.ip = self.config.get('POSTGRES')['ip']

        self.session = SparkSession.builder \
            .master('local') \
            .appName('KOSDAQ Stock Market') \
            .config('spark.driver.extraClassPath', os.path.dirname(__file__) + '/postgresql-42.3.3.jar') \
            .config('spark.mongodb.input.uri', 'mongodb://{0}/'.format(self.ip)) \
            .config('spark.mongodb.output.uri', 'mongodb://{0}/'.format(self.ip)) \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()

    def postgresql_query(self, query):
        info = self.config.get("POSTGRES")
        port, user, pwd, db = 5432, info['user'], info['pw'], 'stockdb'

        df = self.session.read.format('jdbc')\
            .option('url', 'jdbc:postgresql://{0}:{1}/{2}'.format(self.ip,port,db))\
            .option('driver', 'org.postgresql.Driver')\
            .option('query', query)\
            .option('user', user)\
            .option('password', pwd)\
            .load()
        return df

    def mongodb_read(self, db, collection):
        info = self.config.get("MONGO")
        uri = "mongodb://{0}:{1}@{2}:27017/?authSource=admin".format(info['user'], info['pw'], info['ip'])
        df = self.session.read.format('com.mongodb.spark.sql.DefaultSource') \
                .option("uri",uri)\
                .option('database', db)\
                .option('collection', collection) \
                .load()
        return df

    def mongodb_read_date(self, db, collection, date=None):
        # ?????? ??????
        if not date:
            date = datetime.date.today()
        start_datetime, end_datetime = datetime.datetime.combine(date, datetime.time(8, 0, 0)), datetime.datetime.combine(date, datetime.time(15, 30, 0))
        pipeline = {'$match': {'datetime':{'$gte': {'$date': start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")},
                                           '$lte': {'$date': end_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")}}}}
        info = self.config.get("MONGO")
        uri = "mongodb://{0}:{1}@{2}:27017/?authSource=admin".format(info['user'], info['pw'], info['ip'])
        df = self.session.read.format('com.mongodb.spark.sql.DefaultSource') \
                .option("uri",uri)\
                .option('database', db)\
                .option('collection', collection) \
                .option('pipeline', pipeline)\
                .load()
        return df
