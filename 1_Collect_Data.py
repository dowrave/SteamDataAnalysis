import pandas as pd
import numpy as np
import steamspypi
from datetime import datetime, timedelta
import time
import os
# import pymysql
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError, OperationalError
import json

from my_module import get_connection
import mysql_info

COLUMNS_INFO = ['appid', 'name', 'developer', 'publisher', 'initialprice']
COLUMNS_TIME_VALUE = ['appid', 'name', 'date', 'ccu', 'positive', 'negative', 
                        'average_2weeks', 'median_2weeks', 'price', 'discount']
COLUMNS_LANG_GENRE = ['appid', 'name', 'languages', 'genre']
COLUMNS_TAG = ['appid', 'name', 'tags']

DIR_INFO = 'info.csv'
DIR_TIME_VALUE = 'time_value.csv'
DIR_LANG_GENRE = 'lang_genre.csv'
DIR_TAG = 'tag.csv'

DIR_RAW = 'daily_raw_data/'

# MYSQL_DB = mysql_info.db
# MYSQL_DB_RAW = mysql_info.db_raw

# 테스트용 DB
MYSQL_DB = 'STEAMTEST'
MYSQL_DB_RAW = 'STEAMTEST_RAW'

TABLE_INFO = 'info'
TABLE_TIME_VALUE = 'time_value'
TABLE_LANG_GENRE = 'lang_genre'
TABLE_TAG = 'tag'
TABLE_RAW = 'raw'
TABLE_RAW_DETAIL = 'raw_detail'

def check_today_raw_data(by='sql', check_table = TABLE_RAW):
    
    """
    by='sql' : SQL의 raw DB의 raw 데이터를 살펴봄 
    by='csv' : daily_raw_data에 날짜 파일이 있는지 살펴봄
    날짜를 비교해서 데이터를 수집할 필요가 없으면 F, 수집해야 하면 T를 반환
    """
    today = datetime.today().date() - timedelta(days = 1)
    
    if by == 'sql':
        
        try:
            conn = get_connection(db = MYSQL_DB_RAW)
            q = f"""SELECT date FROM {check_table} ORDER BY date DESC LIMIT 1"""
            db_last_day = conn.execute(q).fetchall()[0][0]

            if today == db_last_day:
                print("이미 오늘의 데이터 수집 완료 : 오늘 날짜의 데이터를 df로 꺼냄")
                return False

            else:
                return True

        except (ProgrammingError, IndexError):
            return True

        except OperationalError:
            make_DBs()
            return True
        
    if by == 'csv':
        
        # 이미 csv 파일이 있다면 수집할 필요 없음
        if os.path.exists(DIR_RAW + f'raw_{today.strftime("%Y%m%d")}.csv'):
            return False
        
        # 아니라면 수집해야 함
        return True


def get_steamspy_data(N = 5, to_csv = True, to_sql = True, test = False):

    """
    steamspypi를 이용해 보유자 수 기준 상위 5000개 데이터 수집해서 데이터 프레임 반환
    draft : 테스트 중에는 DIR_RAW에 수집된 df가 있는지를 확인하고 진행함 / 그게 아니라면 
    """
    
    
    # 수집되는 데이터는 어제 날짜로 모인 데이터임.
    today = datetime.today().date() - timedelta(days = 1)
    
    need_to_collect = check_today_raw_data(by = 'sql', check_table = TABLE_RAW)
    
    if need_to_collect == False: # 이미 SQL이나 DB에 데이터가 있다
        
        conn = get_connection(db = MYSQL_DB_RAW)
        
        q = f"""SELECT * FROM {TABLE_RAW} WHERE date = '{today.strftime('%Y-%m-%d')}'"""
        df = pd.read_sql(q, conn)
    
        return df
        
    if test:
        N = 1
    
    print(f"오늘의 {N*1000}개 데이터 수집 시작")
    lst = []
    for i in range(N):
        data_request = {'request' : 'all', 'page' : f'{i}'} 
        data = steamspypi.download(data_request)
        
        # steamspy 사이트 자체가 이상한 경우 False를 반환
        if i == 0:
            try: 
                data['570']
            except KeyError:
                return False
        
        
        temp_df = (pd.DataFrame.from_dict(data, orient = 'index')
                  .reset_index()
                  .drop('index', axis = 1)
         )
        lst.append(temp_df)
        
        # request = all 요청은 1분에 1번씩만 가능
        time.sleep(60)
        
        print(f"{(i + 1) * 1000}개 데이터 수집 완료")
    
    # 데이터 취합 & 필요없는 정보 제거 & 중복 제거
    df = pd.concat(lst) 
    df = df.drop(['userscore', 'owners', 'score_rank'], axis = 1)
    df.loc[:, 'date'] = today.strftime('%Y-%m-%d')
    df = erase_duplicate(df)
    
    if to_csv:
        df.to_csv(f'daily_raw_data/raw_{today.strftime("%Y%m%d")}.csv', index = False)
    
    if to_sql:
        conn = get_connection(db = MYSQL_DB_RAW)
    
        df.to_sql(TABLE_RAW, con = conn, if_exists = 'append', index = False)    
        
        conn.close()
        
    return df


def get_details(appids: iter, to_csv = True, to_sql = True, test = False):
    """
    genre, language, tag를 수집
    """
    need_to_collect = check_today_raw_data(check_table = TABLE_RAW_DETAIL)
    
    if test:
        appids = appids[:100]
        
    if need_to_collect == False:
        conn = get_connection(db = MYSQL_DB_RAW)

        q = f"""SELECT * FROM {TABLE_RAW_DETAIL} WHERE date = '{today.strftime('%Y-%m-%d')}'"""
        detail_df = pd.read_sql(q, conn)
    
    
    
    else:
        print(f"appdetails 수집 - 예정 소요 시간 : {len(appids)}초")

        temp_lst = []
        today = datetime.today().date() - timedelta(days = 1)
        
        
        for count, i in enumerate(appids):

            data_request = {'request' : 'appdetails', 'appid' : f'{i}'}
            data = steamspypi.download(data_request)

            temp_df = pd.DataFrame.from_dict(data, orient = 'index').T

            if type(temp_df) == False:
                print("요청 에러가 발생해서 데이터 수집을 종료합니다")
                return False

            temp_lst.append(temp_df)

            if count % 50 == 0:
                print(i)
                print(f"{count}번째 데이터 작업 중")

        detail_df = pd.concat(temp_lst)
        detail_df = detail_df.drop(['userscore', 'owners', 'score_rank'], axis = 1)

        detail_df.loc[:, 'date'] = today.strftime('%Y-%m-%d')
    
        # detail_df['discount'] = detail_df[detail_df['discount'].isnull()]['discount'].fillna(0)
    
    if to_csv:
        detail_df.to_csv(DIR_RAW + f"detail_{today.strftime('%Y%m%d')}.csv",
                        index = False)
    if to_sql:
        
        temp_df = detail_df.copy()
        temp_df['tags'] = temp_df['tags'].apply(json.dumps) # 이거 때문에 df를 복붙한다
        
        conn = get_connection(db = MYSQL_DB_RAW)
    
        temp_df.to_sql(TABLE_RAW_DETAIL, con = conn, if_exists = 'append', index = False)
        conn.close()
        
    print("수집 종료")
    
    return detail_df

def erase_duplicate(df):
    """
    appid의 중복 제거 : name이 같더라도 appid가 다르다면 다른 게임(pk)으로 간주함
    """
    df_return = df.drop_duplicates(subset = ['appid'])

    return df_return

def add_data_to_db(df, check_data = 'sql', to_csv = True, to_sql = True):
    
    """
    이미 데이터가 있을 때 새로 얻은 데이터들을 나눠서 저장
    인풋 : get_steamspy_data로 얻은 오늘의 데이터프레임
    """
    
    if check_data == 'csv':
        info_df = pd.read_csv(DIR_INFO)
    
    # SQL
    elif check_data == 'sql':
        
        conn = get_connection(db = MYSQL_DB)
        
        q = f"SELECT appid FROM {TABLE_INFO};"
        info_df = pd.read_sql(q, conn)
        conn.close()
    
    # 새로 생긴 appid & 5000위 밖으로 벗어난 appid 정보 얻기
    already_appid = info_df['appid']
    today_appid = df['appid'].unique()
    oldbie_df = df[df['appid'].isin(already_appid)][:]
    newbie_appid = set(today_appid) - set(oldbie_df['appid']) # 기존에 없다가 5000위 내로 진입
    print(f"newbie 데이터 수 : {len(newbie_appid)}개")
    
    outrank_appid = set(already_appid) - set(today_appid) # 기존에 있었으면서 5000위 이탈
    print(f"outrank 데이터 수 : {len(outrank_appid)}개")
    
    additional_appid = newbie_appid | outrank_appid 
    
    additional_appid = list(additional_appid)
    additional_df = get_details(additional_appid, test = test)
    
    # newbie는 새로 생긴 데이터들 -> 3개의 테이블에 추가
    newbie_df = additional_df[additional_df['appid'].isin(newbie_appid)]
    
    # 시간에 관한 테이블은 오늘까지 취합된 모든 데이터에 대해 진행
    oldbie_time_value_df = oldbie_df[COLUMNS_TIME_VALUE]
    additional_time_value_df = additional_df[COLUMNS_TIME_VALUE]
    today_time_value_df = pd.concat([oldbie_time_value_df, additional_time_value_df])
    
    # 저장
    add_no_time_data_to_db(newbie_df, to_csv = to_csv, to_sql = to_sql)
    add_time_data_to_db(today_time_value_df, to_csv = to_csv, to_sql = to_sql)
    
def create_table(df, to_csv = True, to_sql = True, test = False):

    """
    최초 실행 시 오늘의 모든 데이터를 받은 다음, SQL&csv에 저장함
    """
    
    appid = df['appid']
    detail_df = get_details(appid, to_csv = True, to_sql = True, test = test)
    
    info_df = detail_df[COLUMNS_INFO]
    lang_genre_df = detail_df[COLUMNS_LANG_GENRE]
    tag_df = detail_df[COLUMNS_TAG]
    time_value_df = detail_df[COLUMNS_TIME_VALUE]
    
    if to_csv:
        info_df.to_csv(DIR_INFO, index = False)
        lang_genre_df.to_csv(DIR_LANG_GENRE, index = False)
        tag_df.to_csv(DIR_TAG, index = False)
        time_value_df.to_csv(DIR_TIME_VALUE, index = False)
    
    if to_sql:
        
        make_DBs()
        conn = get_connection(db = MYSQL_DB)
        
        info_df.to_sql(TABLE_INFO, con = conn, if_exists = 'append', index = False)
        lang_genre_df.to_sql(TABLE_LANG_GENRE, con = conn, if_exists = 'append', index = False)
        time_value_df.to_sql(TABLE_TIME_VALUE, con = conn, if_exists = 'append', index = False)
        
        tag_df['tags'] = tag_df['tags'].apply(json.dumps)
        tag_df.to_sql(TABLE_TAG, con = conn, if_exists = 'append', index = False)
        conn.close()
        

def add_no_time_data_to_db(detail_df, to_csv = True, to_sql = True):
    """
    get_detail로 얻은 디테일에 대한 데이터 중, 시간에 관계 없는 3개의 테이블에 데이터 추가
    새로운 게임이 생겼을 때만 사용
    """
    info_df = detail_df[COLUMNS_INFO]
    lang_genre_df = detail_df[COLUMNS_LANG_GENRE]
    tag_df = detail_df[COLUMNS_TAG] 
    
    if to_csv:
        info_df.to_csv(DIR_INFO, mode = 'a', index = False, header = False)
        lang_genre_df.to_csv(DIR_LANG_GENRE, mode='a', index = False, header = False)
        tag_df.to_csv(DIR_TAG, mode = 'a', index = False, header = False)
    
    if to_sql:
        
        conn = get_connection(db = MYSQL_DB)
    
        info_df.to_sql(TABLE_INFO, con = conn, if_exists = 'append', index = False)
        lang_genre_df.to_sql(TABLE_LANG_GENRE, con = conn, if_exists = 'append', index = False)
        
        tag_df['tags'] = tag_df['tags'].apply(json.dumps)
        tag_df.to_sql(TABLE_TAG, con = conn, if_exists = 'append', index = False)
        
        conn.close()
        
    
def add_time_data_to_db(detail_df, to_csv = True, to_sql = True):
    """
    get_detail로 얻은 디테일 데이터 중 시간에 관계 있는 테이블에 데이터 추가
    매일 모든 데이터에 대해 실행됨
    """
    
    time_value_df = detail_df[COLUMNS_TIME_VALUE]
    
    if to_csv:
        time_value_df.to_csv(DIR_TIME_VALUE, mode = 'a', index = False, header = False)
    
    if to_sql:
        
        conn = get_connection(db = MYSQL_DB)
        # db_connection_str = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PW}@{MYSQL_HOST}/{MYSQL_DB}'
        # engine = create_engine(db_connection_str, encoding = 'utf-8')
        # conn = engine.connect()
        
        time_value_df.to_sql(TABLE_TIME_VALUE, con = conn, if_exists = 'append', index = False)
        
        conn.close()
        

def check_today_executed(by = 'sql'):
    
    """
    이미 실행되었는가를 판단함 : 가지고 있는 csv 파일이나 테이블을 이용
    0 : 오늘 실행되지 않았으면서 기존에 실행된 적 있음
    1 : 오늘 실행되었음
    2 : 처음 실행
    """
    
    today = datetime.today().date() - timedelta(days = 1)
    
    if by == 'csv':

        if os.path.exists(DIR_TIME_VALUE) == False:
            print("첫 실행")
            return "First"

        # appid와 날짜를 가져오기 위해 time_value에서 데이터를 가져온다
        time_value_df = pd.read_csv(DIR_TIME_VALUE)

        # 날짜가 같다면 실행 X(저장 방식 상 최근 날짜는 마지막에 있음)
        if today.strftime('%Y-%m-%d') == time_value_df['date'].iloc[-1]:
            print("오늘 이미 실행됨")
            return 1
    
        print("오늘 실행된 적 없음")
        return 0
    
    if by == 'sql':
        
        try:
            conn = get_connection(db = MYSQL_DB)

        except OperationalError:
            print("DB가 없음 : 첫 실행")
            return "First"

        try:
            query = "SELECT date FROM time_value ORDER BY 1 DESC LIMIT 1"
            
            last_date = conn.execute(query).fetchall()[0][0]

            if today == last_date:
                print("어제의 정보가 테이블에 이미 있음 : 오늘 이미 실행되었음")
                return 1

            print("어제의 정보가 테이블에 없음 : 데이터 수집 실행")
            return 0

        except (ProgrammingError, TypeError, IndexError): # 데이터가 없는 경우
            print("테이블이 없거나 테이블에 정보가 없음 : 첫 실행")
            return "First"

        

def make_DBs():
    """
    첫 실행 시 create_csv 내에 포함되어 
    MYSQL 4개의 테이블에 포함되는 데이터
    """
    conn = get_connection()
    create_raw_db(conn)
    create_main_db(conn)
    
    conn.close()

def create_raw_db(conn):
    """
    수집한 데이터를 그대로 보관하는 DB를 만듦
    """
    
    # SQLAlchemy를 이용하면 테이블을 파이썬 스크립트로 만들 수 있음
    # 일단 작동엔 이상이 없기 때문에 쿼리 형식을 유지함

    try:
        q = f"CREATE DATABASE {MYSQL_DB_RAW}"
        conn.execute(q)
        q = f"USE {MYSQL_DB_RAW}"
        conn.execute(q)

    except:
        q = f"USE {MYSQL_DB_RAW}"
        conn.execute(q)
        
        
    q_make_raw = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_RAW} (
            appid INT NOT NULL, 
            name VARCHAR(255) NOT NULL,
            developer VARCHAR(255),
            publisher VARCHAR(255),
            positive INT UNSIGNED,
            negative INT UNSIGNED,
            average_forever MEDIUMINT UNSIGNED,
            average_2weeks SMALLINT UNSIGNED,
            median_forever MEDIUMINT UNSIGNED,
            median_2weeks SMALLINT UNSIGNED, 
            price SMALLINT UNSIGNED,
            initialprice INT UNSIGNED,
            discount TINYINT UNSIGNED,
            ccu INT UNSIGNED,
            date DATE NOT NULL
    );
    """

    q_make_raw_detail = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_RAW_DETAIL} (
            appid INT NOT NULL,
            name VARCHAR(255) NOT NULL,
            developer VARCHAR(255),
            publisher VARCHAR(255),
            positive INT UNSIGNED,
            negative INT UNSIGNED,
            average_forever MEDIUMINT UNSIGNED,
            average_2weeks SMALLINT UNSIGNED,
            median_forever MEDIUMINT UNSIGNED,
            median_2weeks SMALLINT UNSIGNED, 
            price SMALLINT UNSIGNED,
            initialprice INT UNSIGNED,
            discount TINYINT UNSIGNED,
            ccu INT UNSIGNED,
            languages VARCHAR(400),
                genre VARCHAR(255),
                tags JSON,
            date DATE NOT NULL
    );
    """
    conn.execute(q_make_raw)
    conn.execute(q_make_raw_detail)    


def create_main_db(conn):
    """가공한 데이터를 보관하는 DB를 만듦"""
    
    try:
        q = f"CREATE DATABASE {MYSQL_DB}"
        conn.execute(q)
        q_1 = f"USE {MYSQL_DB}"
        conn.execute(q_1)
    
    except:
        q_1 = f"USE {MYSQL_DB}"
        conn.execute(q_1)
    
    q_make_info = """
                CREATE TABLE IF NOT EXISTS info (
                appid INT NOT NULL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                developer VARCHAR(255),
                publisher VARCHAR(255),
                initialprice INT
    );
    """
    # time_value는 appid를 PK로 지정하지 않는다!
    q_make_time_value = """
                    CREATE TABLE IF NOT EXISTS time_value (
                    appid INT NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    date DATE NOT NULL,
                    ccu INT UNSIGNED,
                    positive INT UNSIGNED,
                    negative INT UNSIGNED,
                    average_2weeks SMALLINT UNSIGNED,
                    median_2weeks SMALLINT UNSIGNED,
                    price SMALLINT UNSIGNED,
                    discount TINYINT UNSIGNED
                    );
    """
    q_make_lang_genre = """
                    CREATE TABLE IF NOT EXISTS lang_genre (
                    appid INT NOT NULL PRIMARY KEY,
                    name VARCHAR(255),
                    languages VARCHAR(400),
                    genre VARCHAR(255)
                    );
    """
    q_make_tag = """
                CREATE TABLE IF NOT EXISTS tag (
                appid INT NOT NULL PRIMARY KEY,
                name VARCHAR(255),
                tags JSON
                );

    """

    # 테이블 생성(이미 있다면 쿼리 작동 안함)
    conn.execute(q_make_info)
    conn.execute(q_make_time_value)
    conn.execute(q_make_lang_genre)
    conn.execute(q_make_tag)




if __name__ == "__main__":
    TO_CSV = False
    TO_SQL = True
    test = False
    
    if MYSQL_DB == 'STEAMTEST':
        test = True
        print("테스트 중")

    # 0(실행X), 1(실행O), "First"(최초 실행) 반환, 1인 경우 아예 실행 X
    today_executed = check_today_executed(by = 'sql') 

    if today_executed == 0: 
        
        today_df = get_steamspy_data(to_csv = TO_CSV, to_sql = TO_SQL, test = test) # 오늘의 5000개 게임
        
        
        # 사이트가 이상한 경우
        if type(today_df) == bool: 
            print("사이트가 제대로 작동하지 않음 : 갖고 있는 데이터만 수집")
            conn = get_connection(db = MYSQL_DB)

            q = f"SELECT appid FROM {TABLE_INFO};"
            appid = pd.read_sql(q, conn)['appid']
            
            if test:
                appid = appid[:100]
            
            detail_df = get_details(appid, test = test)            
            add_time_data_to_db(detail_df, to_csv = TO_CSV, to_sql = TO_SQL)

        # 사이트가 멀쩡한 경우
        else:
            print("사이트 작동 O, 오늘자 데이터들 갱신")
            add_data_to_db(today_df, to_csv = TO_CSV, to_sql = TO_SQL)
            

    # 아예 처음 실행되는 경우
    elif today_executed == "First":
        
        print("최초 실행")
        
        today_df = get_steamspy_data(to_csv = TO_CSV, to_sql = TO_SQL, test = test)
        
        if type(today_df) == bool: 
            print("현재 정상적으로 조회되지 않는 상황이므로 내일 다시 실행해주세요")
            
        else:
            create_table(today_df, to_csv = TO_CSV, to_sql = TO_SQL, test = test)
    