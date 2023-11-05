"""
스팀에서 지원하는 API를 통해 스팀에 있는 리뷰 데이터를 수집한다.
"""
import time
from datetime import datetime
import mysql_info
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import requests

MYSQL_HOST = mysql_info.host
MYSQL_USER = mysql_info.user
MYSQL_PW = mysql_info.password
MYSQL_PORT = 3306 # 컨테이너에 리뷰를 저장함
MYSQL_DB = mysql_info.db
MYSQL_DB_RAW = mysql_info.db_raw

TABLE_REVIEW_RAW = 'raw_review'

def get_request(url, parameters = None):
    
    try:
        response = requests.get(url = url, 
                               params = parameters)
        
    # SSL 에러 발생 : 수 초후 다시 시도
    except requests.exceptions.SSLError as s:
        print('SSL Error : ', s)
        
        for i in range(5, 0, -1):
            print(f"{i}초 후 재시도", end = '')
            time.sleep(1)
        print("재시도" + ' '*10)
        
    if response:
        return response.json()
    
    else:
        # 보통 리퀘스트가 너무 많이 들어온 경우 발생 : 기다린 후 재시도
        print("10초 후 재실행")
        time.sleep(10)
        print("재시도")
        return get_request(url, parameters)
    
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

def get_reviews(languages: list, target = 'all'):
    """
    target = all이면 info 테이블의 모든 데이터를, \
    아닌 경우는 특정 appid를 입력한다
    """
    
    lst = []
    today = datetime.today().strftime('%Y-%m-%d')
    # languages = ['english', 'japanese', 'korean']
    # languages = ['japanese', 'english', 'korean']
    
    engine = get_engine(db = MYSQL_DB, port = 3306)
    engine_raw = get_engine(db = MYSQL_DB_RAW, port = 3306)
    conn = engine.connect()
    conn_raw = engine_raw.connect()
    
    
    engine2 = get_engine(db = MYSQL_DB, port = 33060)
    engine_raw2 = get_engine(db = MYSQL_DB_RAW, port = 33060)
    conn2 = engine2.connect()
    conn_raw2 = engine_raw2.connect()   
    # INT로 할 필요 없는 값도 INT로 지정함 
    # 예를 들면 게시글 따봉 수 같은 것도 INT UNSIGNED로 지정된 듯한 값들이 보이기 때문임(4294967295)
    # 데이터를 수집하다가 오류가 뜨는 경우가 있어서 그냥 int로 지정해둠
    
    q = f"""CREATE TABLE IF NOT EXISTS {TABLE_REVIEW_RAW} (
            id VARCHAR(16) NOT NULL PRIMARY KEY,
            appid INT UNSIGNED NOT NULL,
            language VARCHAR(20) NOT NULL,
            review VARCHAR(8000) NOT NULL,
            recommend BOOLEAN NOT NULL,
            playtime_forever INT UNSIGNED NOT NULL,
            get_voted_helpful INT UNSIGNED NOT NULL,
            get_voted_funny INT UNSIGNED NOT NULL,
            user_id VARCHAR(17) NOT NULL,
            user_game_owns INT UNSIGNED NOT NULL,
            user_write_reviews INT UNSIGNED NOT NULL,
            playtime_last_two_weeks INT UNSIGNED NOT NULL,
            playtime_at_review INT UNSIGNED NOT NULL,
            last_played_time DATE NOT NULL,
            weighted_vote_score DECIMAL(5, 4) NOT NULL,
            comment_count INT UNSIGNED NOT NULL,
            steam_purchase BOOLEAN NOT NULL,
            received_for_free BOOLEAN NOT NULL,
            written_during_early_access BOOLEAN NOT NULL,
            review_date DATE NOT NULL
    )
    """
    conn_raw.execute(q)
    conn_raw2.execute(q)
    
    q2 = f"""CREATE TABLE IF NOT EXISTS appid_collected (
        appid INT UNSIGNED NOT NULL,
        year VARCHAR(4) NOT NULL,
        month VARCHAR(2) NOT NULL
        )
        """
    conn_raw.execute(q2)
    conn_raw2.execute(q2)
    
    if target == 'all':


        q = 'SELECT appid FROM info'
        appids = pd.read_sql(q, conn)['appid']
        
        # 임시 : 어떤 날짜에 수집하다가 중간에 오류 발생으로 끊겼을 때, 중간부터 시작하기 위한 코드
        # 날짜가 바뀌면 새로운 리뷰가 추가되므로 무의미해질 수 있음
        
        # q = f"SELECT * FROM appid_collected WHERE year = {today[:4]} AND month = {today[5:7]}"
        # collected_appids_this_month = pd.read_sql(q, conn_raw)['appid']
        
        # appids = set(total_appids) - set(collected_appids_this_month)
        # appids = list(appids)
        # appids.sort()
        
        for appid in appids:
                        
            q = "SELECT id FROM raw_review"
            collected_id = pd.read_sql(q, conn_raw)['id']
            print(f"{appid} 수집 시작")
            
            for language in languages:
                print(f"{language} 데이터 수집 중")
                cursor = '*'
                
                # 커서가 디폴트 '*'인 경우, 해당 파라미터 조건으로 검색한 총 데이터 수가 표시됨
                url = f'http://store.steampowered.com/appreviews/{appid}?json=1'
                params = {'language' : f'{language}'}
                json = get_request(url, parameters = params)
                
                number_of_reviews = json['query_summary']['total_reviews']

                reviews_per_page = 100
                total_iterates = (number_of_reviews // reviews_per_page) + 1
                
                if total_iterates >= 50:
                    total_iterates = 50
                
                for i in range(total_iterates):

                    params = {'language' : f'{language}',
                            'day_range' : '365',
                            'cursor' : cursor,
                            'purchase_type' : 'steam',
                            'num_per_page' : reviews_per_page}

                    json = get_request(url, parameters = params)

                    for i in json['reviews']:
                        
                        if np.any(collected_id == i['recommendationid']):
                            continue

                        else:
                            try:
                                lst.append(
                                        [i['recommendationid'], 
                                        appid,
                                        i['language'],
                                        i['review'].replace('\n', ' '), 
                                        i['voted_up'], 
                                        i['author']['playtime_forever'], 
                                        i['votes_up'],
                                        i['votes_funny'],
                                        i['author']['steamid'],
                                        i['author'].get('num_games_owned', 1),
                                        i['author'].get('num_reviews', 1),
                                        i['author'].get('playtime_last_two_weeks', 0),
                                        i['author'].get('playtime_at_review', 0),
                                        datetime.utcfromtimestamp(
                                                    i['author'].get('last_played', today)).strftime('%Y-%m-%d'),
                                        round(float(i['weighted_vote_score']), 4),
                                        i['comment_count'],
                                        i.get('steam_purchase', True),
                                        i.get('received_for_free', False),
                                        i.get('written_during_early_access', False),
                                        datetime.utcfromtimestamp(i['timestamp_updated']).strftime('%Y-%m-%d')
                                        ])

                            except:
                                continue
            
                    cursor = json['cursor']
            
            print(f"현재 {appid}에 관한 새로운 리뷰 갯수 : {len(lst)}개")
            
            # 모은 리스트 데이터프레임 변환 & SQL 저장
            df = pd.DataFrame(lst, columns = ['id', 
                                              'appid', 
                                              'language',
                                              'review', 
                                              'recommend', 
                                              'playtime_forever', 
                                              'get_voted_helpful', 
                                              'get_voted_funny',  
                                              'user_id',
                                              'user_game_owns',
                                              'user_write_reviews',
                                              'playtime_last_two_weeks',
                                              'playtime_at_review',
                                              'last_played_time',
                                              'weighted_vote_score',
                                              'comment_count',
                                              'steam_purchase',
                                              'received_for_free',
                                              'written_during_early_access',
                                              'review_date'])
            df = df.drop_duplicates(subset = ['id'])
            df.to_sql(TABLE_REVIEW_RAW, conn_raw, if_exists = 'append', index = False)
            df.to_sql(TABLE_REVIEW_RAW, conn_raw2, if_exists = 'append', index = False)
            
            lst = []
            cursor = None
            
            q = f"INSERT INTO appid_collected (appid, year, month) VALUE ({appid}, {today[:4]}, {today[5:7]})"
            conn_raw.execute(q)
            conn_raw2.execute(q)
            
            print(f"{appid} 수집 종료")
        
    elif target != 'all':
        
        for language in languages:
            cursor = '*'
            print(f"{target} 수집 시작")

            # 커서가 디폴트 '*'인 경우, 해당 파라미터 조건으로 검색한 총 데이터 수가 표시됨
            url = f'http://store.steampowered.com/appreviews/{target}?json=1'
            params = {'language' : f'{language}'}
            json = get_request(url, parameters = params)

            number_of_reviews = json['query_summary']['total_reviews']
            reviews_per_page = 100
            total_iterates = (number_of_reviews // reviews_per_page) + 1

            for i in range(total_iterates):

                params = {'language' : 'korean',
                        'day_range' : '365',
                        'cursor' : cursor,
                        'purchase_type' : 'steam',
                        'num_per_page' : reviews_per_page}

                json = get_request(url, parameters = params)

                for i in json['reviews']:

                    if np.any(collected_id == i['recommendationid']):
                        continue

                    else:
                        try:
                            lst.append(
                                    [i['recommendationid'], 
                                    appid,
                                    i['language'],
                                    i['review'].replace('\n', ' '), 
                                    i['voted_up'], 
                                    i['author']['playtime_forever'], 
                                    i['votes_up'],
                                    i['votes_funny'],
                                    i['author']['steamid'],
                                    i['author'].get('num_games_owned', 1),
                                    i['author'].get('num_reviews', 1),
                                    i['author'].get('playtime_last_two_weeks', 0),
                                    i['author'].get('playtime_at_review', 0),
                                    datetime.utcfromtimestamp(
                                                i['author'].get('last_played', today)).strftime('%Y-%m-%d'),
                                    round(float(i['weighted_vote_score']), 4),
                                    i['comment_count'],
                                    i.get('steam_purchase', True),
                                    i.get('received_for_free', False),
                                    i.get('written_during_early_access', False),
                                    datetime.utcfromtimestamp(i['timestamp_updated']).strftime('%Y-%m-%d')
                                    ])

                        except:
                            continue

                cursor = json['cursor']

        print(f"현재 {target}에 관한 새로운 리뷰 갯수 : {len(lst)}개")

        # 모은 리스트 데이터프레임 변환 & SQL 저장
        df = pd.DataFrame(lst, columns = ['id', 
                                          'appid', 
                                          'language',
                                          'review', 
                                          'recommend', 
                                          'playtime_forever', 
                                          'get_voted_helpful', 
                                          'get_voted_funny',  
                                          'user_id',
                                          'user_game_owns',
                                          'user_write_reviews',
                                          'playtime_last_two_weeks',
                                          'playtime_at_review',
                                          'last_played_time',
                                          'weighted_vote_score',
                                          'comment_count',
                                          'steam_purchase',
                                          'received_for_free',
                                          'written_during_early_access',
                                          'review_date'])
        df = df.drop_duplicates(subset = ['id'])
        df.to_sql(TABLE_REVIEW_RAW, conn_raw, if_exists = 'append', index = False)

        lst = []
        cursor = None

        q = f"INSERT INTO appid_collected (appid, year, month) VALUE ({appid}, {today[:4]}, {today[5:7]})"
        conn_raw.execute(q)

        print(f"{target} 수집 종료")

    conn.close()
    conn_raw.close()
    engine.dispose()
    engine_raw.dispose()

if __name__ == "__main__":
    languages = ['korean']
    get_reviews(languages, 
                target = 'all') # 특정 게임 or 수집한 전체 게임