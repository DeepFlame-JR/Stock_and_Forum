import sys, os, platform, time
if 'Windows' not in platform.platform():
    os.environ['TZ'] = 'Asia/Seoul'
    time.tzset()
sys.path.append((os.path.dirname(__file__)))

sys.path.append((os.path.dirname(__file__)))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from util import database, common

from sqlalchemy import create_engine
import pandas as pd

class HiveJob(object):
    def __init__(self):
        config = common.Config()
        info = config.get("HIVE")

        self.Log = common.Logger(__file__)
        self.engine = create_engine('hive://%s:10000' % info['ip'])

    def Read(self, sql):
        return pd.read_sql(sql, self.engine)

    def Insert(self, df, db, table):
        df.to_sql(schema=db, name=table, con=self.engine,
                  index=False, method='multi', if_exists='append')

    def CreateTable(self, schema, db, table):
        query = ('''CREATE TABLE IF NOT EXISTS %s.%s %s
        PARTITIONED BY (year int, month int, day int)
        STORED AS PARQUET
        ''') % (db, table, schema)
        self.engine.execute(query)

        df = pd.read_sql("select * from %s.%s" % (db, table), self.engine)
        print(df)

# Create Table
# schema = '(`code` string, ' \
#          '`created` Date, ' \
#          '`name` string, ' \
#          '`market_cap` int, ' \
#          '`price` int,' \
#          '`open_price` int,' \
#          '`high_price` int,' \
#          '`low_price` int,' \
#          '`gap` int,' \
#          '`gap_ratio` float,' \
#          '`trading_volume` int,' \
#          '`institutional_volume` int,' \
#          '`foreign_volume` int,' \
#          '`foreign_ratio` float,' \
#          '`forum_url` string,' \
#          '`forum_count` int,' \
#          '`forum_view` int,' \
#          '`forum_like` int,' \
#          '`forum_unlike`int,' \
#          '`forum_title_length_avg` float,' \
#          '`forum_content_length_avg` float,' \
#          '`forum_reply_count` int)'
# h = HiveJob()
# h.CreateTable(schema, 'stockdb', 'kosdaq')



