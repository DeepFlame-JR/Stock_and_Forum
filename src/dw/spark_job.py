from pyspark.sql import SparkSession, HiveContext
import sys, os, io
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding = 'utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding = 'utf-8')

class SparkJob(object):
    def __init__(self):
        self.ip = '127.0.0.1'
        self.session = SparkSession.builder \
            .master('local') \
            .appName('KOSDAQ Stock Market') \
            .config('spark.driver.extraClassPath', os.path.dirname(__file__) + '/postgresql-42.3.3.jar') \
            .config('spark.mongodb.input.uri', 'mongodb://{0}/'.format(self.ip)) \
            .config('spark.mongodb.output.uri', 'mongodb://{0}/'.format(self.ip)) \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()

    def postgresql_query(self, query):
        port, user, pwd, db = 5432, 'postgres', 'postgres', 'stockdb'

        df = self.session.read.format('jdbc')\
            .option('url', 'jdbc:postgresql://{0}:{1}/{2}'.format(self.ip,port,db))\
            .option('driver', 'org.postgresql.Driver')\
            .option('query', query)\
            .option('user', user)\
            .option('password', pwd)\
            .load()
        return df

    def mongodb_read(self, db, collection):
        df = self.session.read.format('com.mongodb.spark.sql.DefaultSource') \
                .option('database', db)\
                .option('collection', collection) \
                .load()
        # filter = [{eventdtm: '202007080000'}]
        # df = spark.read.format("mongo").option("pipeline", filter).load()
        return df

class SparkforHive:
    def __init__(self):
        self.session = SparkSession.builder\
            .appName('Spark for Hive')\
            .config('hive.metastore.uris', "thrift://localhost:9083")\
            .enableHiveSupport()\
            .getOrCreate()





