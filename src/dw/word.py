from konlpy.tag import Okt
import pyspark

class word:
    def __init__(self):
        self.okt = Okt()

    def get_phrases(self, text, col_name=None):
        if type(text) is pyspark.sql.types.Row and col_name:
            print('Convert!')
            text_dict = text.asDict()[col_name]
            text = text_dict[col_name]

        text = self.okt.normalize(text) # 정규화
        return self.okt.phrases(text)

    def get_pos(self, text):
        if type(text) == 'pyspark.sql.types.Row':
            text = text.asDict()
        text = self.okt.normalize(text) # 정규화
        return self.okt.pos(text)

# text = '나랏말이 중국과 달라 한자와 서로 통하지 아니하므로, \
#     우매한 백성들이 말하고 싶은 것이 있어도 마침내 제 뜻을 잘 표현하지 못하는 사람이 많다.\
#     내 이를 딱하게 여기어 새로 스물여덟 자를 만들었으니, \
#     사람들로 하여금 쉬 익히어 날마다 쓰는 데 편하게 할 뿐이다.'
#
# w = word()
# ans = w.get_phrases(text)
# print(ans)


