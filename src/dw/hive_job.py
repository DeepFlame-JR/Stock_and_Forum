import sys, os, platform, time
if 'Windows' not in platform.platform():
    os.environ['TZ'] = 'Asia/Seoul'
    time.tzset()
sys.path.append((os.path.dirname(__file__)))

sys.path.append((os.path.dirname(__file__)))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from util import database, common

from pyhive import hive
import pandas as pd

class HiveJob(object):
    def __init__(self):
        config = common.Config()
        info = config.get("HIVE")

        self.Log = common.Logger(__file__)
        self.connection = hive.Connection(
            host=info['ip'], port=10000, username=info['user'], password=info['pw'],
            auth='CUSTOM'
            )
        self.cursor = self.connection.cursor()

    def __del__(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def execute(self, query, args = {}):
        self.cursor.execute(query, args)
        try:
            self.cursor.fetchall()
        except Exception as e:
            self.Log.error(e)

def CreateTable(db, name):
    h = HiveJob()
    table_body = '(`code` string, ' \
                 '`date` Date, ' \
                 '`name` string, ' \
                 '`market_cap` int, ' \
                 '`price` int,' \
                 '`open_price` int,' \
                 '`high_price` int,' \
                 '`low_price` int,' \
                 '`gap` int,' \
                 '`gap_ratio` float,' \
                 '`trading_volume` int,' \
                 '`institutional_volume` int,' \
                 '`foreign_volume` int,' \
                 '`foreign_ratio` float,' \
                 '`forum_url` string,' \
                 '`forum_count` int,' \
                 '`forum_view` int,' \
                 '`forum_like` int,' \
                 '`forum_unlike`int,' \
                 '`forum_title_length_avg` float,' \
                 '`forum_content_length_avg` float,' \
                 '`forum_reply_count` int)'
    table_format = ("PARQUET", "TEXTFILE", "AVRO",)
    schema = ('CREATE TABLE IF NOT EXISTS %s.%s %s STORED AS %s') % (db, name, table_body, table_format[0])
    print(schema)
    h.execute(schema)

    df = pd.read_sql("select * from %s.%s"%(db, name), h.connection)
    print(df)

CreateTable('stockdb', 'kosdaq')



