import sys, os, io, platform, time
if 'Windows' not in platform.platform():
    os.environ['TZ'] = 'Asia/Seoul'
    time.tzset()

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sys.path.append((os.path.dirname(__file__)))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from util import database, common

from pyspark.sql import SparkSession, HiveContext

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

class SparkforHive:
    def __init__(self):
        self.session = SparkSession.builder\
            .appName('Spark for Hive')\
            .config('hive.metastore.uris', "thrift://localhost:9083")\
            .enableHiveSupport()\
            .getOrCreate()





