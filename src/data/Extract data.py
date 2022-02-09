import os, datetime
import common

from bs4 import BeautifulSoup
import re, requests

# KOSPI 200 가져오기
def get_KOSPI200():
    time_counter = common.TimeCounter('Get KOSPI200')
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
                data = datetime.date.today(), int(code), name
                result.append(data)

    time_counter.end()
    return result

if __name__ == '__main__':
    kospi200_counter = common.TimeCounter('kospi200 DB')
    db = common.Database('Stock Forum')
    db.insertDB(schema='public', table='kospi200', data=get_KOSPI200())
    kospi200_counter.end()

    print(db.readDB(schema='public', table='kospi200', column='*'))
