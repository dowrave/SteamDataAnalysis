import pandas as pd
from sqlalchemy import create_engine
import mysql_info

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
    
    """sqlalchemy을 이용해 create_engine 객체를 반환"""
    
    if db == None:
        db_connection_str = f'mysql+pymysql://{user}:{password}@{host}:{port}/'
    else:
        db_connection_str = f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}'
    
    engine = create_engine(db_connection_str, encoding = 'utf-8')
    return engine

def unify_columns(df):
    
    """
    원핫인코딩 시 " 언어명"과 "언어명"이 분리됨
    이들을 "언어명"으로 통일함
    """
    print(df)
    for col in df.columns:
        if col[0] == ' ':
            if col.strip() in df.columns:
                df[col.strip()] = (df[[col, col.strip()]].sum(axis = 1)
                                                         .apply(lambda x : 1 if x > 0 else 0))
                df = df.drop(col, axis = 1)

            else:
                df = df.rename(columns = {col: col.strip()})
    
    # 100개 미만의 열 제거
    # df = df.loc[:, df.apply(lambda x : x.sum() >= 100)]
    
    return df

def lang_genre_split_features(raw_lang_genre):
    
    """
    lang_genre의 ','로 구분된 데이터들을 원핫인코딩하여 분리함
    언어는 영어, 한국어, 중국어(간체, 번체), 일본어
    장르는 100개 이상이 있는 것만 남김
    """
    
    languages = (raw_lang_genre.languages.str.get_dummies(sep=',',))
    languages = unify_columns(languages)
    print(languages)
    languages = languages[['English', 'Japanese', 'Korean', "Simplified Chinese", "Traditional Chinese"]]
    
    genre = raw_lang_genre.genre.str.get_dummies(sep=',')
    genre = unify_columns(genre)    
    genre = genre[['Action', 'Adventure', 'Casual', 'Early Access', 'Free to Play', 
               "Indie", "Massively Multiplayer", "RPG", "Racing", "Simulation",
               "Sports", "Strategy"]]    
    
    lang_genre = raw_lang_genre.drop(['genre', 'languages'], axis = 1)    
    lang_genre = pd.merge(lang_genre, languages, left_index = True, right_index = True)
    lang_genre = pd.merge(lang_genre, genre, left_index = True, right_index = True)
    
    return lang_genre

def get_csv_files(get_tag = False):
    """
    컨테이너의 SQL에 저장된 테이블들을 csv로 읽어온다.
    lang_genre의 경우 언어는 영어 + 동북아 언어만 가져오며
    장르도 100개 이상의 게임이 있을 때만 가져온다
    tag의 경우 원핫인코딩 시 테이블이 너무 복잡해져서 성능의 저하가 생기기 때문에 일단은 보류함
    """
    
    engine = get_engine(db = MYSQL_DB)
    conn = engine.connect()
    
    q = f"SELECT * FROM info"
    info = pd.read_sql(q, conn)
    info.to_csv('data/info.csv', index = False)

    q = f"SELECT * FROM time_value"
    time_value = pd.read_sql(q, conn)
    time_value.to_csv('data/time_value.csv', index = False)

    q = f"SELECT * FROM lang_genre"
    lang_genre = pd.read_sql(q, conn)
    lang_genre = lang_genre_split_features(lang_genre)
    lang_genre.to_csv('data/lang_genre.csv', index = False)
    
    if get_tag:
        q = f"SELECT * FROM tag"
        tag = pd.read_sql(q, conn)
        tag.to_csv('data/tag.csv', index = False)
        pass

    engine.dispose()
    
get_csv_files(get_tag = False)