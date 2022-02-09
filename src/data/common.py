import datetime
import time
import psycopg2

class TimeCounter():
    def __init__(self, title):
        self.title = title
        self.start_time = time.time()

    def end(self):
        print("%s: %.5f secs" % (self.title, time.time() - self.start_time))

# DB 클래스 작성
# 참고: https://edudeveloper.tistory.com/131
class Database():
    def __init__(self, name):
        try:
            self.db = psycopg2.connect(host='localhost', dbname=name, user='postgres', password='postgres', port=5432)
            self.db.set_client_encoding('utf-8')
            self.cursor = self.db.cursor()
        except:
            print("Not connected!")

    def __del__(self):
        self.db.close()
        self.cursor.close()

    def execute(self, query, args={}):
        self.cursor.execute(query, args)
        row = self.cursor.fetchall()
        return row

    def commit(self):
        self.cursor.commit()

    def createDB(self, schema, table, datatype):
        sql = " CREATE TABLE {schema}.{table} ({datatype})"\
            .format(schema=schema, table=table, datatype = datatype)
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print("Create Error: ", e)

    def insertDB(self, schema, table, data):
        column_str = '%s,' * len(data[0])
        column_str = '(' + column_str[:-1] + ')'

        args_str = ", ".join([self.cursor.mogrify(column_str, row).decode('utf-8') for row in data])
        sql = "INSERT INTO {schema}.{table} VALUES {data};"\
            .format(schema=schema, table=table, data=args_str)

        # sql = "INSERT INTO {schema}.{table} VALUES {data};"\
        #     .format(schema=schema, table=table, data=column_str)

        try:
            self.cursor.execute(sql)
            # self.cursor.executemany(sql, data)
            self.db.commit()
        except Exception as e:
            print("Insert Error: ", e)

    def readDB(self, schema, table, column):
        sql = " SELECT {column} from {schema}.{table}"\
            .format(column=column, schema=schema, table=table)
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
        except Exception as e:
            result = (" read DB err", e)

        return result

    def updateDB(self,schema,table,column,value,condition):
        sql = " UPDATE {schema}.{table} SET {column}='{value}' WHERE {column}='{condition}' "\
            .format(schema=schema, table=table , column=column ,value=value,condition=condition )
        try :
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            print(" update DB err",e)

    def deleteDB(self, schema, table, condition):
        sql = " delete from {schema}.{table} where {condition} ; "\
            .format(schema=schema, table=table, condition=condition)
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print("delete DB err", e)


# import pandas as pd
#
# db = Database('Stock Forum')
# insert_list = db.readDB(schema='public', table='kospi200', column='*')
# print(pd.DataFrame(insert_list).shape)
#
# counter = TimeCounter('Test for Multi-line insert')
# db.insertDB(schema='public', table='kospi200_2', data=insert_list)
# counter.end()
