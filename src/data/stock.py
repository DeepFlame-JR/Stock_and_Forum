import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from util import database, common

import datetime
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

# KOSDAQ 50 가져오기
# date, code, name, market_cap,
# price, open_price, high_price, low_price,
# gap, gap_ratio, trading_volume,
# institutional_volume, foreign_volume, foreign_ratio, forum_url
# https://appia.tistory.com/734
def get_KOSDAQ50(date):
    def get_KOSDAQ(item):
        try:
            # 기본 정보 가져오기
            forum = item.find_all('a')[1].get('href')
            forum_url = 'https://finance.naver.com' + forum
            k = re.search('\d+', forum)  # 문자열 내 정수만 추출
            code = k.group()
            item_infos = item.get_text().split("\n")
            item_infos = list(map(lambda x: x.replace('\t', '').replace('%', '').replace(',', ''), item_infos))
            item_infos = list(filter(None, item_infos))

            # 주가 가져오기
            # https://joycecoder.tistory.com/107
            params = {
                'symbol': code,
                'requestType': 1,
                'startTime': date.strftime('%Y%m%d'),
                'endTime': date.strftime('%Y%m%d'),
                'timeframe': 'day',
            }
            params = parse.urlencode(params)
            url = "https://api.finance.naver.com/siseJson.naver?%s" % (params)
            r = requests.get(url)
            stock_infos = literal_eval(r.text.strip())[1]

            # 투자자별 매매동향
            r = requests.get('https://finance.naver.com/item/frgn.naver?code=%s'%code, headers={'User-Agent': 'Mozilla/5.0'})
            soup = BeautifulSoup(r.content, 'html.parser')
            trend_item = soup.find('tr', onmouseover="mouseOver(this)")
            trend_infos = trend_item.get_text().split("\n")
            trend_infos = list(map(lambda x: x.replace('\t', '').replace('%', '').replace(',', ''), trend_infos))
            trend_infos = list(filter(None, trend_infos))

            return date, code, item_infos[1], int(item_infos[6]), \
                   stock_infos[4], stock_infos[1], stock_infos[2], stock_infos[3], \
                   int(item_infos[3]), float(item_infos[4]), stock_infos[5], \
                   int(trend_infos[5]), int(trend_infos[6]), stock_infos[6], forum_url
        except Exception as e:
            raise Exception(e, item)
    try:
        time_counter = common.TimeCounter('Get KOSDAQ50')
        url = 'https://finance.naver.com/sise/sise_market_sum.naver?sosok=1&page=1'
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.find('tbody')
        items = table.find_all('tr', onmouseover="mouseOver(this)")
        result = list(map(lambda x: get_KOSDAQ(x), items))
        time_counter.end()
        return result
    except Exception as e:
        print(e)
        return None

if __name__ == '__main__':
    date = datetime.date.today()
    data = get_KOSDAQ50(date)
    if data == None:
        raise Exception('data is None')

    db = database.PostgreSQL('stockdb')
    db.insertDB(schema='public', table='kosdaq', data=data)
    kosdaq_list = db.readDB(schema='public', table='kosdaq', column='*', condition="date='%s'" % date)
    print(len(kosdaq_list))



