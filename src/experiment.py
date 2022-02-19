import time
import psycopg2
import requests, re
from bs4 import BeautifulSoup
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from sqlalchemy.orm.collections import collection

from util import common, database

'''
# Insert 실험
N = 100000
data = [(i,i,i) for i in range(N)]
print('data length:', len(data))

db = psycopg2.connect(host='localhost', dbname='test', user='postgres', password='postgres', port=5432)
cursor = db.cursor()

1번 방법
sql = "INSERT INTO public.inserttest VALUES (%s,%s,%s);"

start_time = time.time()
for row in data:
    try:
        cursor.execute(sql, row)
    except Exception as e:
        print("Insert Error: ", e)

t = time.time()
db.commit()
print(time.time() - t)

print("%s: %.5f secs" % ("Taken Time", time.time() - start_time))

cursor.execute('truncate table public.inserttest restrict')
db.commit()
db.close()
cursor.close()

2번 방법
sql = "INSERT INTO public.inserttest VALUES (%s,%s,%s);"

start_time = time.time()
try:
    cursor.executemany(sql, data)
    db.commit()
except Exception as e:
    print("Insert Error: ", e)

print("%s: %.5f secs" % ("Taken Time", time.time() - start_time))

cursor.execute('truncate table public.inserttest restrict')
db.commit()
db.close()
cursor.close()

3번 방법
args_str = ", ".join([cursor.mogrify('(%s,%s,%s)', row).decode('utf-8') for row in data])
sql = "INSERT INTO public.inserttest VALUES {data};".format(data=args_str)

start_time = time.time()
try:
    cursor.execute(sql)
except Exception as e:
    print("Insert Error: ", e)

db.commit()
print("%s: %.5f secs" % ("Taken Time", time.time() - start_time))

cursor.execute('truncate table public.inserttest restrict')
db.commit()
db.close()
cursor.close()
'''

'''
# response vs driver

# start_time = time.time()
# url = 'https://finance.naver.com/item/board.naver?code=005930'
# r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
# requests_time = time.time()
# soup = BeautifulSoup(r.content, 'html.parser')
# beautiful_time = time.time()
# table = soup.select_one('#content > div.section.inner_sub > table.type2 > tbody')
# end_time = time.time()
# print("%s: %.5f secs" % ("total time", time.time() - start_time))
# print()
# print("%s: %.5f secs" % ("requests.get까지", requests_time - start_time))
# print("%s: %.5f secs" % ("BeatifulSoup까지", beautiful_time - requests_time))
# print("%s: %.5f secs" % ("요소 찾기까지", end_time - beautiful_time))
#
# print(table)

# start_time = time.time()
# url = 'https://finance.naver.com/item/board.naver?code=005930'
# driver = webdriver.Chrome('./data/chromedriver.exe')
# browser_time = time.time()
# driver.get(url)
# driver_get_time = time.time()
# table = driver.find_elements_by_css_selector('#content > div.section.inner_sub > table.type2 > tbody')
# select_time = time.time()
# driver.quit()
# quit_time = time.time()
# print("%s: %.5f secs" % ("total time", time.time() - start_time))
# print()
# print("%s: %.5f secs" % ("브라우저가 열린 때까지", browser_time - start_time))
# print("%s: %.5f secs" % ("url 열 때까지", driver_get_time - browser_time))
# print("%s: %.5f secs" % ("요소 찾기까지", select_time - driver_get_time))
# print("%s: %.5f secs" % ("브라우저가 닫힐 때까지", quit_time - select_time))
# 
# print(table)

# options = webdriver.ChromeOptions()
# options.add_argument('--headless')
# driver = webdriver.Chrome('./data/chromedriver.exe', options=options)
'''

mongo = database.MongoDB()
items = mongo.find_item(db_name='forumdb', collection_name='naverforum')

for item in items:
    print(item['datetime'])
