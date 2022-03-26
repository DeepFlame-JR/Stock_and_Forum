import sys, os, platform, time
if 'Windows' not in platform.platform():
    os.environ['TZ'] = 'Asia/Seoul'
    time.tzset()
sys.path.append((os.path.dirname(__file__)))

sys.path.append((os.path.dirname(__file__)))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from util import database, common

from pyhive import hive
import sasl
import thrift.transport.TSocket
import thrift.transport.TTransport
import thrift_sasl
from thrift.transport.TTransport import TTransportException

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

h = HiveJob()

# Create Test
table_body  = '(`Id` BIGINT, `some_field_1` STRING, `some_field_2` STRING ) '
table_format = ("PARQUET", "TEXTFILE", "AVRO",)
create_tb = ('CREATE TABLE IF NOT EXISTS `%s`.`%s` %s STORED AS %s') % ('stock_db', 'my_table6', table_body, table_format[0])
h.execute(create_tb)

# Read Test
import pandas as pd
df = pd.read_sql("select * from stock_db.my_table2", h.connection)
print(df)
