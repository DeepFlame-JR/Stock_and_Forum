import sys, os
sys.path.append((os.path.dirname(__file__)))
import common
import psycopg2
from pymongo import MongoClient
from pymongo.cursor import CursorType

# Postgre SQL DB 클래스 정의
# 참고: https://edudeveloper.tistory.com/131
class PostgreSQL:
    def __init__(self, name):
        try:
            config = common.Config()
            info = config.get("POSTGRES")
            self.db = psycopg2.connect(host=info['ip'], dbname=name, user=info['user'], password=info['pw'])
            self.db.set_client_encoding('utf-8')
            self.cursor = self.db.cursor()
        except Exception as e:
            print("Not connected!")
            print(e)

    def __del__(self):
        self.db.close()
        self.cursor.close()

    def execute(self, query, args={}):
        self.cursor.execute(query, args)
        row = self.cursor.fetchall()
        return row

    def get_columns(self, table):
        sql = "select column_name from information_schema.columns where table_name='{table}'"\
            .format(table=table)
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            result = list(map(lambda x: x[0], result))
        except Exception as e:
            result = (" read DB err", e)
        return result

    def createDB(self, schema, table, datatype):
        sql = " CREATE TABLE {schema}.{table} ({datatype})"\
            .format(schema=schema, table=table, datatype=datatype)
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print("Create Error: ", e)

    def insertDB(self, schema, table, data):
        counter = common.TimeCounter('Insert %s in PostgreSQL' % table)
        column_str = '%s,' * len(data[0])
        column_str = '(' + column_str[:-1] + ')'

        args_str = ", ".join([self.cursor.mogrify(column_str, row).decode('utf-8') for row in data])
        sql = "INSERT INTO {schema}.{table} VALUES {data};"\
            .format(schema=schema, table=table, data=args_str)
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            self.db.close()
            print("Insert Error: ", e)
        finally:
            counter.end()

    def readDB(self, schema, table, column, condition = None):
        counter = common.TimeCounter('Read %s in PostgreSQL' % table)
        if condition:
            sql = " SELECT {column} from {schema}.{table} where {condition}" \
                .format(column=column, schema=schema, table=table, condition=condition)
        else:
            sql = " SELECT {column} from {schema}.{table}" \
                .format(column=column, schema=schema, table=table)
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
        except Exception as e:
            result = (" read DB err", e)
        finally:
            counter.end()
        return result

    def updateDB(self,schema,table,column,value,condition):
        counter = common.TimeCounter('Update %s in PostgreSQL' % table)
        sql = " UPDATE {schema}.{table} SET {column}='{value}' WHERE {column}='{condition}' "\
            .format(schema=schema, table=table , column=column ,value=value,condition=condition )
        try :
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            print(" update DB err",e)
        finally:
            counter.end()

    def deleteDB(self, schema, table, condition):
        counter = common.TimeCounter('Delete %s in PostgreSQL' % table)
        sql = " delete from {schema}.{table} where {condition} ; "\
            .format(schema=schema, table=table, condition=condition)
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            self.db.close()
            print("delete DB err", e)
        finally:
            counter.end()

# MongoDB SQL DB 클래스 정의
# 참고: https://popcorn16.tistory.com/122
class MongoDB:
    def __init__(self):
        config = common.Config()
        info = config.get("MONGO")
        self.client = MongoClient("mongodb://{0}:{1}@{2}:27017/?authSource=admin"
                                  .format(info['user'], info['pw'], info['ip'])
                                  )

    def insert_item_one(self, data, db_name=None, collection_name=None):
        counter = common.TimeCounter('Insert %s in MongoDB' % collection_name)
        result = self.client[db_name][collection_name].insert_one(data).inserted_id
        counter.end()
        return result

    def insert_item_many(self, datas, db_name=None, collection_name=None):
        counter = common.TimeCounter('Insert %s in MongoDB' % collection_name)
        result = self.client[db_name][collection_name].insert_many(datas).inserted_ids
        counter.end()
        return result

    def find_item_one(self, condition=None, db_name=None, collection_name=None):
        result = self.client[db_name][collection_name].find_one(condition, {"_id": False})
        return result

    def find_item(self, condition=None, db_name=None, collection_name=None):
        result = self.client[db_name][collection_name].find(condition, {"_id": False}, no_cursor_timeout=True, cursor_type=CursorType.EXHAUST)
        return result

    def delete_item_one(self, condition=None, db_name=None, collection_name=None):
        result = self.client[db_name][collection_name].delete_one(condition)
        return result

    def delete_item_many(self, condition=None, db_name=None, collection_name=None):
        result = self.client[db_name][collection_name].delete_many(condition)
        return result

    def update_item_one(self, condition=None, update_value=None, db_name=None, collection_name=None):
        result = self.client[db_name][collection_name].update_one(filter=condition, update=update_value)
        return result

    def update_item_many(self, condition=None, update_value=None, db_name=None, collection_name=None):
        result = self.client[db_name][collection_name].update_many(filter=condition, update=update_value)
        return result

    def text_search(self, text=None, db_name=None, collection_name=None):
        result = self.client[db_name][collection_name].find({"$text": {"$search": text}})
        return result
