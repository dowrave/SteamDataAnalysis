import pandas as pd
from sqlalchemy import create_engine

from my_module import get_connection
import mysql_info

MYSQL_DB = mysql_info.db
MYSQL_DB_RAW = mysql_info.db_raw

def unify_columns(df):
    
    """
    원핫인코딩 시 " 언어명"과 "언어명"이 분리됨 / 
    이들을 "언어명"으로 통일함
    """
    
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
    lang_genre의 ','로 구분된 데이터들을 원핫인코딩하여 분리함 /
    언어는 영어, 한국어, 중국어(간체, 번체), 일본어 / 
    장르는 100개 이상이 있는 것만 남김
    """
    
    languages = (raw_lang_genre.languages.str.get_dummies(sep=',',))
    languages = unify_columns(languages)
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

conn = get_connection(db = MYSQL_DB)

raw_lang_genre = pd.read_sql("SELECT * FROM lang_genre", conn)
lang_genre = lang_genre_split_features(raw_lang_genre)
lang_genre.to_csv('processed_lang_genre.csv', index = False)


