import datetime
import os, platform, time
if 'Windows' not in platform.platform():
    os.environ['TZ'] = 'Asia/Seoul'
    time.tzset()

import common, database
import psycopg2
from pymongo import MongoClient
from pymongo.cursor import CursorType

def postgres_migration():
    config = common.Config()
    info = config.get("POSTGRES")
    local_db = psycopg2.connect("host='{0}' dbname='{1}' user='{2}'  password='{3}'" \
                                 .format('localhost', 'stockdb', info['user'], info['pw']))
    local_cursor = local_db.cursor()
    sql = "select * from kosdaq"
    local_cursor.execute(sql)
    local_result = local_cursor.fetchall()
    db = database.PostgreSQL('stockdb')
    db.insertDB(schema='public', table='kosdaq', data=local_result)

    local_db.close()
    local_cursor.close()

timezone_kst = datetime.timezone(datetime.timedelta(hours=9))
def mongodb_edit():
    mongo = database.MongoDB()
    result = mongo.find_item_one(db_name='forumdb', collection_name='naverforum')
    print(result['title'])
    result['datetime'] = result['datetime'].astimezone(timezone_kst)
    mongo.insert_item_one(result, db_name='forumdb', collection_name='test')

