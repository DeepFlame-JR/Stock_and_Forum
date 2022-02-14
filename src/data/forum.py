import datetime
import common
import pandas as pd

from bs4 import BeautifulSoup
from urllib import parse
from ast import literal_eval
import re, requests

# date, title, id, view, like, dislike + response(like, dislike, response)
def get_forum(url, start_date, end_date):
    def get_response(url):
        return 0

    def get_forum_row(item):
        item_infos = item.get_text().split("\n")
        item_infos = list(map(lambda x: x.replace('\t', '').replace(',', ''), item_infos))
        item_infos = list(filter(None, item_infos))
        date = datetime.datetime.strptime(item_infos[0], '%Y.%m.%d %H:%M'),

        if len(item_infos) == 7:
            # print(get_response(item.a.get('href')))
            return date, item_infos[1], int(item_infos[2].replace('[','').replace(']','')), \
                   item_infos[-4], int(item_infos[-3]), int(item_infos[-2]), int(item_infos[-1])
        else:
            return date, item_infos[1], 0,\
                   item_infos[-4], int(item_infos[-3]), int(item_infos[-2]), int(item_infos[-1])

    time_counter = common.TimeCounter('Get Forum Time')
    url += '&page=1'
    r = requests.get(url, headers={'User-Agent':'Mozilla/5.0'})
    soup = BeautifulSoup(r.content, 'html.parser')
    table = soup.select_one('#content > div.section.inner_sub > table.type2 > tbody')
    items = table.find_all('tr', onmouseover="mouseOver(this)")

    result = list(map(lambda x: get_forum_row(x), items))
    time_counter.end()
    return result

if __name__ == '__main__':
    # db = common.PostgreSQL('stockdb')
    # kosdaq_list = db.readDB(schema='public', table='kodaq', column='*',
    #                           condition="date='%s'" % (datetime.date.today()))

    t = get_forum('https://finance.naver.com/item/board.naver?code=091990',0,0)
    print(t, sep='\n')