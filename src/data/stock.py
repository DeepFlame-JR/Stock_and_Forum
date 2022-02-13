import datetime
import common
import pandas as pd

from bs4 import BeautifulSoup
from urllib import parse
from ast import literal_eval
import re, requests

# KOSPI 200 가져오기 (date, stock_code, stock_name)
def get_KOSPI200():
    time_counter = common.TimeCounter('Get KOSPI200 Time')
    result = []

    BaseURL = 'https://finance.naver.com/sise/entryJongmok.nhn?&page='
    for i in range(1, 21):
        url = BaseURL + str(i)
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'lxml')
        items = soup.find_all('td', {'class':'ctg'})

        for item in items:
            txt = item.a.get('href')
            k = re.search('\d+', txt) # 문자열 내 정수만 추출
            if k:
                code = k.group()
                name = item.text
                data = datetime.date.today(), code, name
                result.append(data)

    time_counter.end()
    return result

# KOSDAQ 50 가져오기 (date, name, code, price, gap, gap_ratio, trading_volume, market_cap, foreign_ratio, forum_url)
# https://appia.tistory.com/734
def get_KOSDAQ50():
    def get_KOSDAQ(item):
        try:
            forum = item.find_all('a')[1].get('href')
            forum_url = 'https://finance.naver.com' + forum
            k = re.search('\d+', forum)  # 문자열 내 정수만 추출
            code = k.group()

            item_infos = item.get_text().split("\n")
            item_infos = list(map(lambda x: x.replace('\t', '').replace('%', '').replace(',', ''), item_infos))
            item_infos = list(filter(None, item_infos))
            return datetime.date.today(), code, item_infos[1],  \
                   int(item_infos[2]), int(item_infos[3]), float(item_infos[4]), int(item_infos[9]), \
                   int(item_infos[6]), float(item_infos[8]), forum_url
        except Exception as e:
            print(e)
            return None
    time_counter = common.TimeCounter('Get KOSDAQ50 Time')
    url = 'https://finance.naver.com/sise/sise_market_sum.naver?sosok=1&page=1'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    table = soup.find('tbody')
    items = table.find_all('tr', onmouseover="mouseOver(this)")
    result = list(map(lambda x: get_KOSDAQ(x), items))
    time_counter.end()
    return result

# 주가 가져오기
# https://joycecoder.tistory.com/107
def get_stocks(kospi200_list, date = datetime.date.today()):
    def get_stock(code, start_time, end_time, time_frame='day'):
        params = {
            'symbol': code,
            'requestType': 1,
            'startTime': start_time.strftime('%Y%m%d'),
            'endTime': end_time.strftime('%Y%m%d'),
            'timeframe': time_frame,
        }
        params = parse.urlencode(params)
        url = "https://api.finance.naver.com/siseJson.naver?%s" % (params)
        r = requests.get(url)
        return tuple([start_time, code] + literal_eval(r.text.strip())[1][1:])

    time_counter = common.TimeCounter('Get KOSPI200 Time')
    result = list(map(lambda x: get_stock(x, date, date + datetime.timedelta(days=1)), kospi200_list))
    time_counter.end()
    return result

if __name__ == '__main__':
    db = common.PostgreSQL('stockdb')
    db.insertDB(schema='public', table='kodaq', data=get_KOSDAQ50())
    kosdaq_list = db.readDB(schema='public', table='kodaq', column='*',
                              condition="date='%s'" % (datetime.date.today()))
    print(kosdaq_list)