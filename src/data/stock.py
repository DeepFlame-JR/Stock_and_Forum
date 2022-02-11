import datetime
import common
import pandas as pd

from bs4 import BeautifulSoup
from urllib import parse
from ast import literal_eval
import re, requests

# KOSPI 200 가져오기
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

# KOSDAQ 50 가져오기 (date, stock_code, stock_name, market_cap)
def get_KOSDAQ():

    return 0

# 주가 가져오기
# https://joycecoder.tistory.com/107
def get_stocks(kospi200_list, date = datetime.date.today()):
    time_counter = common.TimeCounter('Get KOSPI200 Time')
    result = list(map(lambda x: get_stock(x, date, date + datetime.timedelta(days=1)), kospi200_list))
    time_counter.end()
    return result

def get_stock(code, start_time, end_time, time_frame='day'):
    params = {
        'symbol':code,
        'requestType':1,
        'startTime':start_time.strftime('%Y%m%d'),
        'endTime':end_time.strftime('%Y%m%d'),
        'timeframe':time_frame,
    }
    params = parse.urlencode(params)
    url = "https://api.finance.naver.com/siseJson.naver?%s"%(params)
    r = requests.get(url)
    return tuple([start_time, code] + literal_eval(r.text.strip())[1][1:])

if __name__ == '__main__':
    db = common.Database('stockdb')
    db.insertDB(schema='public', table='kospi200', data=get_KOSPI200())
    kospi200_list = db.readDB(schema='public', table='kospi200', column='stock_code',
                              condition="date='%s'" % (datetime.date.today()))
    kospi200_list = list(map(lambda x: x[0], kospi200_list))
    db.insertDB(schema='public', table='stock', data=get_stocks(kospi200_list))