import time
import psycopg2
import requests, re, common
from bs4 import BeautifulSoup
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By

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

# 네이버 조목토론방
try:

    url = 'https://finance.naver.com/item/board_read.naver?code=091990&nid=214154852&st=&sw=&page=2'

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome('./chromedriver.exe')#, options=options)
    driver.get(url)

    # counter1 = common.TimeCounter('test')

    # reply_count = item.find('span',{'class' : 'u_cbox_reply_cnt'})
    # counter1.end()
    #
    # print(reply_count)
    # print(int(reply_count.text))
    # print(item)

    result = dict()

    counter2 = common.TimeCounter('test')
    ass = driver.find_elements_by_tag_name('a')

    replies = driver.find_elements_by_class_name('u_cbox_btn_reply')
    for reply in replies:
        reply_count = int(reply.text.split()[1])
        if reply_count > 0:
            reply.click()
            time.sleep(0.05)

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    content = soup.select_one('#body')

    response = soup.find('div', {'class':'u_cbox_content_wrap'})
    items = response.find_all('li')

    item = items[7]
    content = item.find('span', {'class':'u_cbox_contents'})
    # if content == None:
    #     return
    result['content'] = content.text
    result['like'] = int(item.find('em', {'class':'u_cbox_cnt_recomm'}).text)
    result['dislike'] = int(item.find('em', {'class': 'u_cbox_cnt_unrecomm'}).text)

    sub_response = []
    lis = item.find_all('li')
    for li in lis:
        temp = dict()
        content = li.find('span', {'class': 'u_cbox_contents'})
        if content == None:
            continue
        temp['content'] = content.text
        temp['like'] = int(item.find('em', {'class':'u_cbox_cnt_recomm'}).text)
        temp['dislike'] = int(item.find('em', {'class': 'u_cbox_cnt_unrecomm'}).text)
        sub_response.append(temp)
    result['response'] = sub_response
    print(result)
    counter2.end()

except Exception as e:
    print(e)
finally:
    time.sleep(1)
    # driver.quit()

# # url = 'https://finance.naver.com/item/board.naver?code=091990'
# r = requests.get(url, headers={'User-Agent':'Mozilla/5.0'})
# print(r.text)
# soup = BeautifulSoup(r.text, 'lxml')
# # print(soup)
# # print(table)