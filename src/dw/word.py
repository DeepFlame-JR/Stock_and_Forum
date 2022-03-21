import sys, os, platform, time
if 'Windows' not in platform.platform():
    os.environ['TZ'] = 'Asia/Seoul'
    time.tzset()

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from util import database, common

from konlpy.tag import Okt
import pyspark
from pyspark.sql import Row

class word:
    def __init__(self):
        self.okt = Okt()
        self.Log = common.Logger(__file__)

    def test(self, row):
        row_dict = row.asDict()
        row_dict['tt'] = row_dict['title'] + "_test"
        newRow = Row(**row_dict)
        return newRow

    def get_phrases_row(self, row, col_name):
        if type(row) is Row and col_name:
            row_dict = row.asDict()
            text = row_dict[col_name]
            result = self.get_phrases(text)
            row_dict[col_name+'_phrases'] = len(result)
            newRow = Row(**row_dict)
            return newRow

    def get_phrases(self, text):
        text = self.okt.normalize(text) # 정규화
        return self.okt.phrases(text)

    def get_pos(self, text):
        text = self.okt.normalize(text) # 정규화
        return self.okt.pos(text)

# d = {'title':'나랏말이 중국과 달라 한자와 서로 통하지 아니하므로'}
# r = pyspark.sql.Row(**d)
#
# w = word()
# ans = w.get_phrases_row(r, 'title')
# print(ans)

# text = '나랏말이 중국과 달라 한자와 서로 통하지 아니하므로, \
#     우매한 백성들이 말하고 싶은 것이 있어도 마침내 제 뜻을 잘 표현하지 못하는 사람이 많다.\
#     내 이를 딱하게 여기어 새로 스물여덟 자를 만들었으니, \
#     사람들로 하여금 쉬 익히어 날마다 쓰는 데 편하게 할 뿐이다.'
#
# w = word()
# ans = w.get_phrases(text)
# print(ans)


