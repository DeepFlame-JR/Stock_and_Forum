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

        self.connection = hive.Connection(
            host=info['ip'], port=10000, username=info['user'], password=info['pw'],
            auth='CUSTOM'
            )
        self.cursor = self.connection.cursor()

    def __del__(self):
        if self.connection:
            self.connection.close()
        if self.cursor:
            self.cursor.close()

    def execute(self, query, args = {}):
        self.cursor.execute(query, args)
        row = self.cursor.fetchall()
        return row

h = HiveJob()