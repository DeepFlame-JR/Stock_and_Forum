import sys, os, platform, time
if 'Windows' not in platform.platform():
    os.environ['TZ'] = 'Asia/Seoul'
    time.tzset()

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from util import database, common

import datetime
from bs4 import BeautifulSoup
import requests

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from seleniumrequests import Chrome
from webdriver_manager.chrome import ChromeDriverManager

Log = common.Logger(__file__)
inTime = True
driver = None

# datetime, date, title, id, view, like, dislike + reply(like, dislike, response)
def get_forum(code, name, forum_url, start_datetime, end_datetime):
    global Log, inTime, driver

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
        global Log, inTime, driver

        item_infos = item.get_text().split("\n")
        item_infos = list(map(lambda x: x.replace('\t', '').replace(',', ''), item_infos))
        item_infos = list(filter(None, item_infos))
        date_time = datetime.datetime.strptime(item_infos[0], '%Y.%m.%d %H:%M')
        if not start_datetime <= date_time <= end_datetime:
            if date_time < start_datetime:
                inTime &= False
            return None

        driver.get('https://finance.naver.com' + item.a.get('href'))
        # ?????? ????????? ?????? ?????? ?????????
        buttons = driver.find_elements(by=By.CLASS_NAME, value='u_cbox_btn_reply')
        for button in buttons:
            reply_count = int(button.text.split()[1])
            if reply_count > 0:
                button.click()
                time.sleep(0.05)

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        content = soup.select_one('#body').getText()
        title = soup.select_one('#content > div.section.inner_sub > table.view > tbody > tr:nth-child(1) > th:nth-child(1) > strong').getText()

        response = soup.find('div', {'class': 'u_cbox_content_wrap'})
        items = response.find_all('li')
        reply_list = list(map(lambda x:get_reply(x), items))
        reply_list = list(filter(None, reply_list))

        row_dict = {'name' : name, 'code': code, 'datetime':date_time,
                    'title': item_infos[1], 'content':content, 'id':item_infos[-4],
                    'view':int(item_infos[-3]), 'like':int(item_infos[-2]), 'unlike':int(item_infos[-1]),
                    'reply':reply_list, 'reply_count':len(reply_list)}
        return row_dict

    forum_list = []
    page = 0
    inTime = True
    while inTime:
        page += 1
        r = requests.get(forum_url + '&page=%d' % page, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(r.text, 'html.parser')
        table = soup.select_one('#content > div.section.inner_sub > table.type2 > tbody')
        items = table.find_all('tr', onmouseover="mouseOver(this)")
        result = list(map(lambda x: get_forum_row(x), items))
        result = list(filter(None, result))
        forum_list.extend(result)
    return forum_list

def main_get_forum(start, end, port):
    global Log, inTime, driver

    driver = None
    forum_counter = common.TimeCounter('Get Forum Time')
    try:
        Log.info('start to get forum data')

        date = datetime.date.today()
        # date = datetime.date(2022,4,7)
        # KOSDAQ ????????????
        postgres = database.PostgreSQL('stockdb')
        kosdaq_list = postgres.readDB(schema='public', table='kosdaq', column='date, code, name, forum_url',
                                        condition="date='%s'" % date)
        if len(kosdaq_list) == 0:
            raise Exception('today is not the opening date')

        # ?????? ??????
        today, yesterday = date, date + datetime.timedelta(days=-1)
        start_datetime, end_datetime = datetime.datetime.combine(today, datetime.time(8,0,0)), datetime.datetime.combine(today, datetime.time(15,30,0))

        # ????????? KOSDAQ ????????? ??????????????? ????????? ?????????
        options = webdriver.ChromeOptions()
        Log.info('Open browser')
        if 'Windows' not in platform.platform():
            config = common.Config()
            info = config.get("MONGO")
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument("--single-process")
            options.add_argument("--disable-dev-shm-usage")
            proxy = "%s:%s" % (info['ip'], port)
            webdriver.DesiredCapabilities.CHROME['proxy'] = {
                "httpProxy":proxy,
                "ftpProxy":proxy,
                "sslProxy":proxy,
                "proxyType": "MANUAL",
            }
            webdriver.DesiredCapabilities.CHROME['acceptSslCerts'] = True
            driver = webdriver.Remote('http://%s:%s'%(info['ip'], port), options=options)
        else:
            driver = Chrome(service=Service(ChromeDriverManager().install()), chrome_options=options)

        Log.info('Open Mongo DB')
        mongo = database.MongoDB()
        for i, stock in enumerate(kosdaq_list):
            if start <= i < end:
                date, code, name, forum_url = stock
                inner_counter = common.TimeCounter(name + '(' + str(i+1) + '/' + str(len(kosdaq_list)) + ')')

                # retires ??????
                condition = {'name':name, 'datetime':{'$gte':start_datetime, '$lte':end_datetime}}
                check_data = mongo.find_item(condition=condition, db_name='forumdb', collection_name='naverforum')
                data_length = len(list(check_data))
                if data_length != 0:
                    inner_counter.end('data is already inserted')
                    continue

                forum = get_forum(code, name, forum_url, start_datetime, end_datetime)
                if len(forum) > 0:
                    mongo.insert_item_many(datas=forum, db_name='forumdb', collection_name='naverforum')
                inner_counter.end(str(len(forum)) + '??? ')

    except Exception as e:
        Log.error(e)
    finally:
        if driver:
            driver.quit()
            Log.info("driver end")
        forum_counter.end()