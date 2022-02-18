import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from util import database, common

import datetime
import pandas as pd

from bs4 import BeautifulSoup
from urllib import parse
import re, requests
from selenium import webdriver
import time

inTime = True
driver = None

# date, title, id, view, like, dislike + reply(like, dislike, response)
def get_forum(code, name, forum_url, start_date, end_date):
    global inTime, driver

    def get_reply(item):
        result = dict()

        content = item.find('span', {'class': 'u_cbox_contents'})
        if content == None:
            return None
        result['content'] = content.text
        result['like'] = int(item.find('em', {'class': 'u_cbox_cnt_recomm'}).text)
        result['dislike'] = int(item.find('em', {'class': 'u_cbox_cnt_unrecomm'}).text)

        sub_response = []
        lis = item.find_all('li')
        for li in lis:
            temp = dict()
            content = li.find('span', {'class': 'u_cbox_contents'})
            if content == None:
                continue
            temp['content'] = content.text
            temp['like'] = int(item.find('em', {'class': 'u_cbox_cnt_recomm'}).text)
            temp['dislike'] = int(item.find('em', {'class': 'u_cbox_cnt_unrecomm'}).text)
            sub_response.append(temp)
        result['response'] = sub_response
        return result

    def get_forum_row(item):
        global inTime, driver

        item_infos = item.get_text().split("\n")
        item_infos = list(map(lambda x: x.replace('\t', '').replace(',', ''), item_infos))
        item_infos = list(filter(None, item_infos))
        date = datetime.datetime.strptime(item_infos[0], '%Y.%m.%d %H:%M')
        if not start_date <= date <= end_date:
            if date < start_date:
                inTime &= False
            return None

        driver.get('https://finance.naver.com' + item.a.get('href'))
        # 답글 버튼이 있는 경우 누르기
        buttons = driver.find_elements_by_class_name('u_cbox_btn_reply')
        for button in buttons:
            reply_count = int(button.text.split()[1])
            if reply_count > 0:
                button.click()
                time.sleep(0.05)

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        content = soup.select_one('#body').getText()

        response = soup.find('div', {'class': 'u_cbox_content_wrap'})
        items = response.find_all('li')
        reply_list = list(map(lambda x:get_reply(x), items))
        reply_list = list(filter(None, reply_list))

        row_dict = {'name' : name, 'code': code, 'date':date,
                    'title': item_infos[1], 'content':content, 'id':item_infos[-4],
                    'view':int(item_infos[-3]), 'like':int(item_infos[-2]), 'unlike':int(item_infos[-1]),
                    'reply':reply_list, 'reply_count':len(reply_list)}
        return row_dict

    try:
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        driver = webdriver.Chrome('./chromedriver.exe', options=options)

        forum_list = []
        page = 0
        inTime = True
        while inTime:
            page += 1
            r = requests.get(forum_url + '&page=%d' % page, headers={'User-Agent': 'Mozilla/5.0'})
            soup = BeautifulSoup(r.content, 'html.parser')
            table = soup.select_one('#content > div.section.inner_sub > table.type2 > tbody')
            items = table.find_all('tr', onmouseover="mouseOver(this)")

            result = list(map(lambda x: get_forum_row(x), items))
            result = list(filter(None, result))
            forum_list.extend(result)

        return forum_list
    except Exception as e:
        print(e)
    finally:
        if driver:
            driver.quit()

if __name__ == '__main__':
    # KOSDAQ 불러오기
    sqlDB = database.PostgreSQL('stockdb')
    kosdaq_list = sqlDB.readDB(schema='public', table='kodaq', column='date, code, name, forum_url',
                              condition="date='%s'" % (datetime.date.today()))

    # 일자 설정
    today, yesterday = datetime.date.today(), datetime.date.today() - datetime.timedelta(days=-1)
    start_date, end_date = datetime.datetime.combine(today, datetime.time(8,0,0)), datetime.datetime.combine(today, datetime.time(15,30,0))

    # 불러온 KOSDAQ 종목의 종목토론방 데이터 크롤링
    nosqlDB = database.MongoDB()
    forum_counter = common.TimeCounter('Get Forum Time')
    for stock in kosdaq_list[4:]:
        date, code, name, forum_url = stock
        inner_counter = common.TimeCounter(name)
        forum = get_forum(code, name, forum_url, start_date, end_date)
        if len(forum) > 0:
            nosqlDB.insert_item_many(datas=forum, db_name='forumdb', collection_name='naverforum')
        inner_counter.end(str(len(forum)) + '개 ')
    forum_counter.end()


