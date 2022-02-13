import time
import psycopg2
import requests, re
from bs4 import BeautifulSoup
import datetime

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

url = 'https://finance.naver.com/item/board_read.naver?code=091990&nid=213764637&st=&sw=&page=4'
r = requests.get(url)
soup = BeautifulSoup(r.content, 'html.parser')
print(soup)
# table = soup.select_one('#cbox_module_wai_u_cbox_content_wrap_tabpanel')
# print(table)