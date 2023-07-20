# 가상환경에 Java를 깔고 JAVA_HOME 환경변수를 설정해야 실행이 가능함
# 현재(230710) 가상환경에 Java를 설치하지 않았기 때문에 실행 불가능(로컬에서 작성한 코드)

from konlpy.tag import *
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import mysql_info
from collections import Counter
import re
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import seaborn as sns

MYSQL_HOST = mysql_info.host
MYSQL_USER = mysql_info.user
MYSQL_PW = mysql_info.password
MYSQL_PORT = 3306

MYSQL_DB = mysql_info.db
MYSQL_DB_RAW = mysql_info.db_raw

def get_engine(host = MYSQL_HOST,
                user = MYSQL_USER,
                password = MYSQL_PW,
               port = MYSQL_PORT,
                db = None):
    
    """sqlalchemy을 이용해 create_engine.connect() 객체를 반환"""
    
    if db == None:
        db_connection_str = f'mysql+pymysql://{user}:{password}@{host}:{port}/'
    else:
        db_connection_str = f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}'
    
    engine = create_engine(db_connection_str, encoding = 'utf-8')    
    return engine

engine = get_engine(db = MYSQL_DB)
conn = engine.connect()
engine_raw = get_engine(db = MYSQL_DB_RAW)
conn_raw = engine_raw.connect()

q = "SELECT appid, name FROM info"
temp = pd.read_sql(q, conn)

# DAVE THE DIVER를 알아보자
target_appid = temp[temp['name'] == 'DAVE THE DIVER']['appid'].values[0]

q = f"SELECT * FROM raw_review WHERE appid = {target_appid}"
df = pd.read_sql(q, conn_raw)

df = df[['id', 'review', 'recommend', 'playtime_forever', 'review_date']]
df['review_length'] = df['review'].apply(lambda x : len(x))

hannanum = Hannanum()
kkma = Kkma()
komoran = Komoran()
# mecab = Mecab() # 윈도우에서 실행 불가능이라는 듯
okt = Okt()

examples = df.loc[:, 'review']


han_lst = []
kkm_lst = []
kom_lst = []
okt_lst = []


# lst.extend : 현재 리스트에 다른 리스트의 원소들을 추가함
for example in examples:
    
    # okt가 8000자를 만나니까 아예 이상이 생겨서 넣어봄
    if len(example) >= 4000:
        example = example[:2000]
    
    # 공백을 제외한 특수문자 제거
    p = re.compile('[^ㄱ-ㅎ가-힣a-zA-Z0-9\s]')
    example = re.sub(p, '', example)
    
    han_morph = hannanum.morphs(example)
    kkm_morph = kkma.morphs(example)
    kom_morph = komoran.morphs(example)
    okt_morph = okt.morphs(example)
    
    # 한 명이 쓴 리뷰에서, 같은 단어가 여러 번 나오더라도 1개만 카운트함
    han_morph = list(set(han_morph))
    kkm_morph = list(set(kkm_morph))
    kom_morph = list(set(kom_morph))
    okt_morph = list(set(okt_morph))
    
    # 1글자는 일반적으로 형태소나 의미 없는 조사인 경우가 많아서 제외해봄
    han_result = [i for i in han_morph if len(i) >= 2]
    kkm_result = [i for i in kkm_morph if len(i) >= 2]
    kom_result = [i for i in kom_morph if len(i) >= 2]
    okt_result = [i for i in okt_morph if len(i) >= 2]
    
    han_lst.extend(han_result)
    kkm_lst.extend(kkm_result)
    kom_lst.extend(kom_result)
    okt_lst.extend(okt_result)
    
    
han_count = Counter(han_lst)
kkm_count = Counter(kkm_lst)
kom_count = Counter(kom_lst)
okt_count = Counter(okt_lst)

fig, ax = plt.subplots(2, 2, figsize = (10, 10))


for i, lst in enumerate([han_count, kkm_count, kom_count, okt_count]):
    for j in range(2):
        wc = WordCloud(font_path='malgun', 
                       width=400, 
                       height=400, 
                       scale=2.0, 
                       max_font_size=250,
                      background_color = 'white')
        
        img = wc.generate_from_frequencies(lst)
        ax[i // 2][j].imshow(img)
        ax[i // 2][j].set_axis_off()

plt.show()

conn.close()
conn_raw.close()
engine.dispose()
engine_raw.dispose()