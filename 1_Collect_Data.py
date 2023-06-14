import pandas as pd
import numpy as np
import steamspypi
from datetime import datetime, timedelta
import time
import os
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError, OperationalError
import json
import schedule
from my_module import get_connection
import mysql_info
import sys
from requests.exceptions import RequestException
# import pymysql # sqlalchemy에서 사용하기 떄문에 설치는 필요함

pd.set_option('mode.chained_assignment',  None)

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

MYSQL_HOST = mysql_info.host
MYSQL_USER = mysql_info.user
MYSQL_PW = mysql_info.password
MYSQL_PORT = 3306
MYSQL_DB = mysql_info.db
MYSQL_DB_RAW = mysql_info.db_raw

# 테스트용 DB
# MYSQL_DB = 'STEAMTEST'
# MYSQL_DB_RAW = 'STEAMTEST_RAW'

TABLE_INFO = 'info'
TABLE_TIME_VALUE = 'time_value'
TABLE_LANG_GENRE = 'lang_genre'
TABLE_TAG = 'tag'
TABLE_RAW = 'raw'
TABLE_RAW_DETAIL = 'raw_detail'
TABLE_RAW_CHECKPOINT = 'checkpoint'

# 컨테이너 버전은 얘도 함수에 같이 포함시켰다. 차이가 있는 거 유의하면서 작업하자.
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


def check_today_raw_data(conn, by='sql', check_table = TABLE_RAW):
    
    """
    by='sql' : SQL의 raw DB의 raw 데이터를 살펴봄 
    by='csv' : daily_raw_data에 날짜 파일이 있는지 살펴봄
    날짜를 비교해서 데이터를 수집할 필요가 없으면 F, 수집해야 하면 T를 반환
    """
    today = datetime.today().date() - timedelta(days = 1)
    
    if by == 'sql':
        
        try:
            
            q = f"""SELECT date FROM {check_table} ORDER BY date DESC LIMIT 1"""
            db_last_day = conn.execute(q).fetchall()[0][0]

            if today == db_last_day:
                print("이미 오늘의 데이터 수집 완료 : 오늘 날짜의 데이터를 df로 꺼냄")
                return False

            else:
                return True

        except (ProgrammingError, IndexError): # IndexError : 데이터가 없는 경우
            return True

        except OperationalError:
            return True
        
    if by == 'csv':
        
        # 이미 csv 파일이 있다면 수집할 필요 없음
        if os.path.exists(DIR_RAW + f'raw_{today.strftime("%Y%m%d")}.csv'):
            return False
        
        # 아니라면 수집해야 함
        return True


def get_steamspy_data(conn, N = 5, to_csv = True, to_sql = True, test = False):

    """
    steamspypi를 이용해 보유자 수 기준 상위 5000개 데이터 수집해서 데이터 프레임 반환
    """
    
    # 수집되는 데이터는 어제 날짜로 모인 데이터임.
    today = datetime.today().date() - timedelta(days = 1)
    
    need_to_collect = check_today_raw_data(conn)
    
    if need_to_collect == False: # 이미 SQL이나 DB에 데이터가 있다
        
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
    df = df.drop_duplicates(subset = ['appid'])
    if to_csv:
        df.to_csv(f'daily_raw_data/raw_{today.strftime("%Y%m%d")}.csv', index = False)
    
    if to_sql:
        df.to_sql(TABLE_RAW, con = conn, if_exists = 'append', index = False)    

        
    return df


def log_checkpoint(conn_raw, idx):
    """
    get_detail 중 steamspy와의 통신과정에서 오류가 발생했을 경우
    오류가 발생한 인덱스를 raw db의 checkpoint에 저장함
    """
    q = f"INSERT INTO {TABLE_RAW_CHECKPOINT} (checkpoint_idx) VALUES (%s)"
    conn_raw.execute(q, (idx,))

    
def load_checkpoint(conn_raw):
    
    try:
        q = f"SELECT checkpoint_idx FROM {TABLE_RAW_CHECKPOINT}"
        checkpoint = conn_raw.execute(q).fetchone()[0]
        return checkpoint
    
    except TypeError:
        return False


def del_checkpoint(conn_raw):
    q = f"DELETE FROM {TABLE_RAW_CHECKPOINT}"
    conn_raw.execute(q)



def check_today_raw_detail_data(conn_raw, appids):
    """
    체크포인트에 값이 있으면 중간에 실행 정지된 거라서 데이터를 더 수집해야 함
    체크포인트에 값이 없는 경우, 추가로 체크해야 할 항목
        - RAW_DETAIL의 어제 날짜에 저장된 길이가 appids에 들어온 길이와 같은가?
        - 같다면 모두 수집되었으므로 수집 필요 x
        - 다르다면 오늘 실행된 적이 없는 거니까 수집해야 함
    """
    today = datetime.today().date() - timedelta(days = 1)
    idx = load_checkpoint(conn_raw)
    
    # 중단된 적 있는 경우
    if idx == True:
        return True
    
    else:
        q = f"SELECT COUNT(*) FROM {TABLE_RAW_DETAIL} WHERE date = '{today}'"
        check = conn_raw.execute(q).fetchone()[0]
        
        if len(appids) == check:
            return False
        
        else:
            return True

def save_detail_to_sql(conn, lst, today):
    """
    get_detail() 에서만 쓰임 / 수집된 raw_detail 데이터 sql에 저장
    """
    detail_df = pd.concat(lst)
    detail_df = detail_df.drop(['userscore', 'owners', 'score_rank'], axis = 1)
    detail_df.loc[:, 'date'] = today.strftime('%Y-%m-%d')

    temp_df = detail_df.copy()
    temp_df['tags'] = temp_df['tags'].apply(json.dumps) # 이거 때문에 df를 복붙한다

    temp_df.to_sql(TABLE_RAW_DETAIL, 
                con = conn, 
                if_exists = 'append', 
                index = False)
        
        
def get_details(conn_raw, appids: iter, to_csv = False, to_sql = True, test = False):
    """
    genre, language, tag를 수집 -> "Raw_detail"에 저장
    """
    
    need_to_collect = check_today_raw_detail_data(conn_raw, appids)
    
    
    if test:
        appids = appids[:100]
    
    
    if need_to_collect == False:
        q = f"""SELECT * FROM {TABLE_RAW_DETAIL} WHERE date = '{today.strftime('%Y-%m-%d')}'"""
        detail_df = pd.read_sql(q, conn_raw)
        return detail_df
    
    
    else:
        
        checkpoint_idx = load_checkpoint(conn_raw)
        if checkpoint_idx:
            appids = appids[checkpoint_idx:]
        
        
        print(f"appdetails 수집 - 예정 소요 시간 : {len(appids)}초")

        temp_lst = []
        today = datetime.today().date() - timedelta(days = 1)

        for count, i in enumerate(appids):
            
            try:
                data_request = {'request' : 'appdetails', 'appid' : f'{i}'}
                data = steamspypi.download(data_request)

                temp_df = pd.DataFrame.from_dict(data, orient = 'index').T

                if type(temp_df) == False:
                    print("요청 에러가 발생해서 데이터 수집을 종료합니다")
                    return False

                temp_lst.append(temp_df)

                if count > 0:
                    if count % 50 == 0:
                        print(i)
                        print(f"{count}번째 데이터 작업 중")

                    if count % 500 == 0 and to_sql:
                        print("500개마다 저장합니다")
                        
                        save_detail_to_sql(conn_raw, temp_lst, today)
                        
                        # detail_df = pd.concat(temp_lst)
                        # detail_df = detail_df.drop(['userscore', 'owners', 'score_rank'], axis = 1)
                        # detail_df.loc[:, 'date'] = today.strftime('%Y-%m-%d')

                        # temp_df = detail_df.copy()
                        # temp_df['tags'] = temp_df['tags'].apply(json.dumps) # 이거 때문에 df를 복붙한다

                        # temp_df.to_sql(TABLE_RAW_DETAIL, 
                        #             con = conn_raw, 
                        #             if_exists = 'append', 
                        #             index = False)
                        
                        temp_lst = []
                
            except RequestException:
                
                # steamspy에서 불러오는 함수에 에러가 발생한 경우를 가정함
                # 현재 temp_lst에 있는 데이터들을 저장하고, 체크포인트를 넣어둠
                
                save_detail_to_sql(conn_raw, temp_lst, today)
                
                del_checkpoint(conn_raw) 
                log_checkpoint(conn_raw, count)
                
                print("에러가 발생해서 체크포인트를 저장하고 프로그램이 일시 종료됩니다")
                
                sys.exit(1)
        
        # 마지막 수집 이후 저장
        
        save_detail_to_sql(conn_raw, temp_lst, today)
    
        if to_csv:
            detail_df.to_csv(DIR_RAW + f"detail_{today.strftime('%Y%m%d')}.csv",
                            index = False)
        if to_sql:
            
            # 500개씩 나눠서 저장한 데이터들을 한꺼번에 불러옴
            q = f"SELECT * FROM {TABLE_RAW_DETAIL} WHERE date = '{today}'"
            today_detail_df = pd.read_sql(q, conn_raw)

            del_checkpoint(conn_raw) 
        
            print("수집 종료")
            
            return today_detail_df

def add_data_to_db(conn, conn_raw, today_df, check_data = 'sql', to_csv = True, to_sql = True):
    
    """
    이미 데이터가 있을 때 새로 얻은 데이터들을 나눠서 저장
    인풋 : get_steamspy_data로 얻은 오늘의 데이터프레임
    """
    
    # 이미 info 테이블에 있는 appid 얻기
    if check_data == 'csv':
        info_df = pd.read_csv(DIR_INFO)
    
    elif check_data == 'sql':
        q = f"SELECT appid FROM {TABLE_INFO};"
        info_df = pd.read_sql(q, conn)
    
    # 오늘 새로 생긴 appid와 순위에서 벗어난 appid 수집
    already_appid = info_df['appid']
    today_appid = today_df['appid']
    
    oldbie_df = today_df[today_df['appid'].isin(already_appid)][:]
    newbie_appid = set(today_appid) - set(oldbie_df['appid']) # 기존에 없다가 5000위 내로 진입
    outrank_appid = set(already_appid) - set(today_appid) # 기존에 있었으면서 5000위 이탈
    
    print(f"새로운 데이터 수 : {len(newbie_appid)}개")
    print(f"순위 이탈 데이터 수 : {len(outrank_appid)}개")
    
    # 순위 이탈 & 진입 데이터를 수집
    additional_appid = newbie_appid | outrank_appid 
    additional_appid = list(additional_appid)
    additional_df = get_details(conn_raw, additional_appid, test = test)
    
    # newbie는 새로 생긴 데이터들 -> 3개의 테이블에 추가
    newbie_df = additional_df[additional_df['appid'].isin(newbie_appid)]
    
    # 시간에 관한 테이블은 오늘까지 취합된 모든 데이터에 대해 진행
    oldbie_time_value_df = oldbie_df[COLUMNS_TIME_VALUE]
    additional_time_value_df = additional_df[COLUMNS_TIME_VALUE]
    today_time_value_df = pd.concat([oldbie_time_value_df, additional_time_value_df])
    
    # 저장
    add_no_time_data_to_db(conn, newbie_df, to_csv = to_csv, to_sql = to_sql)
    add_time_data_to_db(conn, today_time_value_df, to_csv = to_csv, to_sql = to_sql)


def add_no_time_data_to_db(conn, detail_df, to_csv = True, to_sql = True):
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
        
        q = f'USE {MYSQL_DB}'
        conn.execute(q)
        
        info_df.to_sql(TABLE_INFO, con = conn, if_exists = 'append', index = False)
        lang_genre_df.to_sql(TABLE_LANG_GENRE, con = conn, if_exists = 'append', index = False)
        
        tag_df['tags'] = tag_df['tags'].apply(json.dumps)
        tag_df.to_sql(TABLE_TAG, con = conn, if_exists = 'append', index = False)
        
          
def add_time_data_to_db(conn, detail_df, to_csv = True, to_sql = True):
    """
    get_detail로 얻은 디테일 데이터 중 시간에 관계 있는 테이블에 데이터 추가
    매일 모든 데이터에 대해 실행됨
    """
    
    time_value_df = detail_df[COLUMNS_TIME_VALUE]
    
    if to_csv:
        time_value_df.to_csv(DIR_TIME_VALUE, mode = 'a', index = False, header = False)
    
    if to_sql:
        
        q = f'USE {MYSQL_DB}'
        conn.execute(q)

        time_value_df.to_sql(TABLE_TIME_VALUE, con = conn, if_exists = 'append', index = False)

        
def create_table(conn, conn_raw, df, to_csv = True, to_sql = True, test = False):

    """
    최초 실행 시 오늘의 모든 데이터를 받은 다음, SQL&csv에 저장함
    """
    
    appid = df['appid']
    detail_df = get_details(conn_raw, appid, to_csv = True, to_sql = True, test = test)
    
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
        
        info_df.to_sql(TABLE_INFO, con = conn, if_exists = 'append', index = False)
        lang_genre_df.to_sql(TABLE_LANG_GENRE, con = conn, if_exists = 'append', index = False)
        time_value_df.to_sql(TABLE_TIME_VALUE, con = conn, if_exists = 'append', index = False)
        
        tag_df['tags'] = tag_df['tags'].apply(json.dumps)
        tag_df.to_sql(TABLE_TAG, con = conn, if_exists = 'append', index = False)
        


        
def check_CS_GO_values(conn):
    
    """
    날짜가 변경되더라도 steamspy 사이트가 갱신되지 않았을 수 있음(갱신 시간을 모름)
    따라서 가장 변동이 있을 확률이 높은 카스 글옵을 조사함
    데이터에 변동이 있다면 수집, 아니면 오늘 이미 수집된 것으로 판단함.
    현재까지 get=all이 이상한 적은 있었어도 appdetails 수집은 정상적이었기 때문에 이런 가정으로 접근함.
    """
        
    q = f"""SELECT * FROM {TABLE_TIME_VALUE} WHERE appid = 730 ORDER BY date DESC LIMIT 1"""
    df = pd.read_sql(q, conn)
    df = df.drop('date', axis = 1)

    data_request = {'request' : 'appdetails', 'appid' : f'{730}'}
    data = steamspypi.download(data_request)
    
    if (df.positive[0] == data['positive'] and
        df.negative[0] == data['negative'] and
        df.ccu[0] == data['ccu'] and
        df.average_2weeks[0] == data['average_2weeks'] and
        df.median_2weeks[0] == data['median_2weeks']):
        return True
    else:
        return False
    

def check_today_executed(conn, by = 'sql'):
        
    """
    이미 실행되었는가를 판단함 : 가지고 있는 csv 파일이나 테이블을 이용
    0 : 오늘 실행되지 않았으면서 기존에 실행된 적 있음
    1 : 오늘 실행되었음
    First : 처음 실행
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
            executed = check_CS_GO_values(conn)
            
            if executed:
                print("웹의 정보와 SQL의 정보가 동일함")
                return 1
            
            else:
                print("웹의 정보와 SQL의 정보가 다름")
                return 0
            
        except (ProgrammingError, TypeError, IndexError): # 데이터가 없는 경우
            print("테이블이 없거나 테이블에 정보가 없음 : 첫 실행")
            return "First"

def create_raw_db(conn):
    """
    수집한 데이터를 그대로 보관하는 DB를 만듦
    """
    
    # SQLAlchemy를 이용하면 테이블을 파이썬 스크립트로 만들 수 있음
    # 일단 작동엔 이상이 없기 때문에 쿼리 형식을 유지함

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
    
    q_make_checkpoint = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_RAW_CHECKPOINT} (
            id SMALLINT AUTO_INCREMENT PRIMARY KEY,
            checkpoint_idx INT
        )
    """
    
    conn.execute(q_make_raw)
    conn.execute(q_make_raw_detail)
    conn.execute(q_make_checkpoint)

def create_main_db(conn):
    """가공한 데이터를 보관하는 DB를 만듦"""
    
    
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

def get_engine_connect():
    """
    2개의 DB와 커넥트를 만듦
    DB가 없다면 만듦
    """
    
    try:
        engine = get_engine(db = MYSQL_DB)
        engine_raw = get_engine(db = MYSQL_DB_RAW)
        
        conn = engine.connect()
        conn_raw = engine_raw.connect()
        
    except OperationalError as e:
        
        error_code = e.orig.args[0]
        
        if error_code == 1049: # DB가 없는 경우 만들고, 다시 연결해준다
            
            engine_temp = get_engine()
            conn_temp = engine_temp.connect()
            
            q = f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}"
            q_raw = f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB_RAW}"
            
            conn_temp.execute(q)
            conn_temp.execute(q_raw)
            
            conn_temp.close()
            engine_temp.dispose()
            
            engine = get_engine(db = MYSQL_DB)
            engine_raw = get_engine(db = MYSQL_DB_RAW)

            conn = engine.connect()
            conn_raw = engine_raw.connect()
            
            create_main_db(conn)
            create_raw_db(conn_raw)
    
    return engine, engine_raw, conn, conn_raw
    
    
def main_func(test = False, TO_CSV = False, TO_SQL = True):
    
    engine, engine_raw, conn, conn_raw = get_engine_connect()
    
    # 0(실행X), 1(실행O), "First"(최초 실행) 반환, 1인 경우 아예 실행 X
    today_executed = check_today_executed(conn, by = 'sql') 

    if today_executed == 0: 

        today_df = get_steamspy_data(conn_raw, to_csv = TO_CSV, to_sql = TO_SQL, test = test) # 오늘의 5000개 게임

        # 사이트가 이상한 경우
        if type(today_df) == bool: 
            
            print("사이트가 제대로 작동하지 않음 : 갖고 있는 데이터만 수집")
            q = f"SELECT appid FROM {TABLE_INFO};"
            appid = pd.read_sql(q, conn)['appid'].sort_values()

            detail_df = get_details(conn_raw, appid, test = test)            
            add_time_data_to_db(conn, detail_df, to_csv = TO_CSV, to_sql = TO_SQL)

        # 사이트가 멀쩡한 경우
        else:
            print("사이트 작동 O, 오늘자 데이터들 갱신")
            add_data_to_db(conn, conn_raw, today_df, to_csv = TO_CSV, to_sql = TO_SQL)


    # 아예 처음 실행되는 경우
    elif today_executed == "First":

        print("최초 실행")

        today_df = get_steamspy_data(conn_raw, to_csv = TO_CSV, to_sql = TO_SQL, test = test)

        if type(today_df) == bool: 
            print("현재 정상적으로 조회되지 않는 상황이므로 내일 다시 실행해주세요")

        else:
            create_table(conn, conn_raw, today_df, to_csv = TO_CSV, to_sql = TO_SQL, test = test)

            
    conn.close()
    conn_raw.close()
    engine.dispose()
    engine_raw.dispose()
    
    print("6시간 후에 재실행됨")

test = False

if MYSQL_DB != 'steam':
    test = True # test = True라면 100개의 데이터만 detail 수집함 / 이미 있다면 그냥 이용함
    print("테스트 중")

main_func(test, TO_CSV = False, TO_SQL = True)
schedule.every(6).hours.do(main_func)
while True:
    schedule.run_pending()
    time.sleep(1)