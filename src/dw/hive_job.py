from pyhive import hive
import mock
import sasl
import thrift.transport.TSocket
import thrift.transport.TTransport
import thrift_sasl
from thrift.transport.TTransport import TTransportException

class HiveJob(object):
    def __init__(self):
        socket = thrift.transport.TSocket.TSocket('localhost', 9083)
        sasl_auth = 'PLAIN'
        sasl_client = sasl.Client()
        sasl_client.setAttr('host', 'localhost')
        sasl_client.setAttr('username', 'hive')
        sasl_client.setAttr('password', 'hive')
        sasl_client.init()

        transport = thrift_sasl.TSaslClientTransport(sasl_client, sasl_auth, socket)
        self.connection = hive.Connection(thrift_transport=transport)
        self.cursor = self.connection.cursor()

    def __del__(self):
        self.connection.close()
        self.cursor.close()

    def execute(self, query, args = {}):
        self.cursor.execute(query, args)
        row = self.cursor.fetchall()
        return row
