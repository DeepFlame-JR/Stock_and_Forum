import sys, os, platform, time
if 'Windows' not in platform.platform():
    os.environ['TZ'] = 'Asia/Seoul'
    time.tzset()

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(__file__))
from util import database, common

from konlpy.tag import Okt
import pyspark
from pyspark.sql import Row

okt = Okt()
Log = common.Logger(__file__)

def get_phrases(text):
    text = okt.normalize(text)  # 정규화
    return okt.phrases(text)

def get_pos(text):
    text = okt.normalize(text)  # 정규화
    return okt.pos(text)

def get_phrases_row(row, col_name):
    if type(row) is Row and col_name:
        row_dict = row.asDict()
        text = row_dict[col_name]
        result = get_phrases(text)
        row_dict[col_name + '_phrases'] = result
        newRow = Row(**row_dict)
        return newRow

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


