import pandas as pd
import numpy as np
import steamspypi
from datetime import datetime, timedelta
import time
import os

# pd.set_option('display.max_rows', 500)
# pd.set_option('display.max_columns', 100)
# pd.set_option('display.max_colwidth', 500)

INFO_COLUMNS = ['appid', 'name', 'developer', 'publisher', 'initialprice']
TIME_VALUE_COLUMNS = ['appid', 'name', 'date', 'ccu', 'positive', 'negative', 
                        'average_2weeks', 'median_2weeks', 'price', 'discount']
LANG_GENRE_COLUMNS = ['appid', 'name', 'languages', 'genre']
TAG_COLUMNS = ['appid', 'name', 'tags']

INFO_DIR = 'data/info.csv'
TIME_VALUE_DIR = 'data/time_value.csv'
LANG_GENRE_DIR = 'data/lang_genre.csv'
TAG_DIR = 'data/tag.csv'

def get_steamspy_data(N = 5, save = False):

    """
    steamspypi를 이용해 보유자 수 기준 상위 5000개 데이터 수집해서 데이터 프레임 반환
    """
    
    lst = []
    
    # 오늘 조사하는 데이터는 코드 실행 하루 전 날짜를 대상으로 집계된 데이터 
    today = datetime.today().date() - timedelta(days = 1)

    for i in range(N):
        data_request = {'request' : 'all', 'page' : f'{i}'} 
        data = steamspypi.download(data_request)
        
        # steamspy 사이트 자체가 이상한 경우 False를 반환
        if i == 0:
            try: 
                data['570']
            except KeyError:
                print("steampypi 에러 발생 -> appdetails로 데이터 수집")
                return False
        
        
        temp_df = (pd.DataFrame.from_dict(data, orient = 'index')
                  .reset_index()
                  .drop('index', axis = 1)
         )
        lst.append(temp_df)
        
        # request = all 요청은 1분에 1번씩만 가능
        time.sleep(60)
        
        print(f"{(i + 1) * 1000}개 데이터 수집 완료")
        
    df = pd.concat(lst) 
    
    df = df.drop(['userscore', 'owners', 'score_rank'], axis = 1)
    df.loc[:, 'date'] = today.strftime('%Y-%m-%d')

    df = erase_duplicate(df)
    
    if save:
        df.to_csv(f'daily_raw_data/raw_{today.strftime("%Y%m%d")}.csv', index = False)
    
    return df

def check_duplicate(df, col: str):
    
    """
    appid, name에 사용, 중복 시 그 값들을 보여줌
    """
    
    unique_counts = df[col].nunique()
    print(f"레코드 수 = {df.shape[0]}, {col}의 unique 값 갯수 = {df[col].nunique()}")
    
    if df.shape[0] != unique_counts:
        mask = df[col].value_counts() >= 2
        dupl_df = df[df[col].map(mask)].head(unique_counts).sort_values(col)
        display(dupl_df)

    else:
        print(f"{col} 칼럼에 대해 모든 레코드가 유일함")    


def get_details(appids: iter):
    """
    genre, language, tag를 수집
    """
    print(f"예정 소요 시간 : {len(appids)}초")
    
    temp_lst = []
    today = datetime.today().date() - timedelta(days = 1)
    for count, i in enumerate(appids):

        data_request = {'request' : 'appdetails', 'appid' : f'{i}'}
        data = steamspypi.download(data_request)
        temp_df = pd.DataFrame.from_dict(data, orient = 'index').T
        temp_lst.append(temp_df)

        if count % 100 == 0:
            print(i)
            print(f"{count}번째 데이터 작업 중")

    detail_df = pd.concat(temp_lst)
    detail_df.loc[:, 'date'] = today.strftime('%Y-%m-%d')
    
    print("수집 종료")
    
    
    return detail_df

def erase_duplicate(df):
    """
    appid, name의 중복 제거
    name의 경우 ccu(최고동접자수)가 높은 데이터만을 남김
    """
    df_return = df.drop_duplicates(subset = ['appid'])
    
    df_return = df_return.sort_values('ccu', ascending = False)
    df_return = df_return.drop_duplicates(subset = ['name'])
    
    return df_return

def add_data_to_csv(df):
    """
    이미 데이터가 있을 때 새로 얻은 데이터들을 나눠서 저장
    인풋 : get_steamspy_data로 얻은 오늘의 데이터프레임
    """
    
    # appid와 날짜를 가져오기 위해 time_value에서 데이터를 가져온다
    time_value_df = pd.read_csv(TIME_VALUE_DIR, encoding_errors = 'ignore')
    
    already_appid = set(time_value_df['appid'])
    today_appid = today_df['appid']
    
    # oldbie : 오늘 5000위 내에 있고, info.csv에 있는 데이터
    oldbie_df = today_df[today_df['appid'].isin(already_appid)]
    
    # newbie : 오늘의 info에 없는 새로운 데이터
    newbie_appid = set(today_appid) - set(oldbie_df['appid'])
    
    # outrank : info.csv에 있었지만 순위 밖으로 벗어난 데이터
    outrank_appid = set(already_appid) - set(today_appid) 

    additional_appid = newbie_appid | outrank_appid 
    additional_df = get_details(additional_appid)

    # newbie는 새로 생긴 데이터들 -> 3개의 테이블에 추가
    newbie_df = additional_df[additional_df['appid'].isin(newbie_appid)]
    
    # 시간에 관한 테이블은 오늘까지 취합된 모든 데이터에 대해 진행
    oldbie_time_value_df = oldbie_df[TIME_VALUE_COLUMNS]
    additional_time_value_df = additional_df[TIME_VALUE_COLUMNS]
    today_time_value_df = pd.concat([oldbie_time_value_df, additional_time_value_df])
    
    # 저장
    add_no_time_data_csv(newbie_df)
    add_time_data_csv(today_time_value_df)
    
def create_csv(df):

    """
    최초 실행 시 파일을 저장함
    """
    
    appid = df['appid']
    detail_df = get_details(appid)
    
    info_df = detail_df[INFO_COLUMNS]
    lang_genre_df = detail_df[LANG_GENRE_COLUMNS]
    tag_df = detail_df[TAG_COLUMNS]
    time_value_df = detail_df[TIME_VALUE_COLUMNS]
    
    info_df.to_csv(INFO_DIR, index = False)
    lang_genre_df.to_csv(LANG_GENRE_DIR, index = False)
    tag_df.to_csv(TAG_DIR, index = False)
    time_value_df.to_csv(TIME_VALUE_DIR, index = False)

    
def add_no_time_data_csv(detail_df):
    """
    get_detail로 얻은 디테일에 대한 데이터 중, 시간에 관계 없는 3개의 테이블에 데이터 추가
    새로운 게임이 생겼을 때만 사용
    """
    info_df = detail_df[INFO_COLUMNS]
    lang_genre_df = detail_df[LANG_GENRE_COLUMNS]
    tag_df = detail_df[TAG_COLUMNS] 
    
    info_df.to_csv(INFO_DIR, mode = 'a', index = False, header = False)
    lang_genre_df.to_csv(LANG_GENRE_DIR, mode='a', index = False, header = False)
    tag_df.to_csv(TAG_DIR, mode = 'a', index = False, header = False)
    
def add_time_data_csv(detail_df):
    """
    get_detail로 얻은 디테일 데이터 중 시간에 관계 있는 테이블에 데이터 추가
    매일 모든 데이터에 대해 실행됨
    """
    time_value_df = detail_df[TIME_VALUE_COLUMNS]
    time_value_df.to_csv(TIME_VALUE_DIR, mode = 'a', index = False, header = False)
    
def check_executed():
    """
    이미 실행되었는가를 판단함
    0 : 오늘 실행되지 않았으면서 기존에 실행된 적 있음
    1 : 오늘 실행되었음
    2 : 처음 실행
    """
    today = datetime.today().date() - timedelta(days = 1)
    
    if os.path.exists(TIME_VALUE_DIR) == False:
        print("첫 실행")
        return "First"
    
    # appid와 날짜를 가져오기 위해 time_value에서 데이터를 가져온다
    time_value_df = pd.read_csv(TIME_VALUE_DIR, encoding_errors = 'ignore')
    
    # 날짜가 같다면 실행 X(저장 방식 상 최근 날짜는 마지막에 있음)
    if today.strftime('%Y-%m-%d') == time_value_df['date'].iloc[-1]:
        print("오늘 이미 실행됨")
        return 1
    
    print("오늘 실행된 적 없음")
    return 0


if __name__ == "__main__":
    today_executed = check_executed() # 오늘 실행되었는가

    if today_executed == 0: 
        
        today_df = get_steamspy_data(save = True) # 저장 파일명 : raw_[실행날짜-1].csv
        
        if type(today_df) == bool: 
            appid = pd.read_csv(INFO_DIR)['appid']
            detail_df = get_details(appid)
            add_time_data_csv(detail_df)
            
        else:
            # time_value_df가 있다는 건 이미 실행되었다는 뜻이므로 추가로 점검할 필요 X
            add_data_to_csv(today_df)
        
    elif today_executed == "First":
        
        today_df = get_steamspy_data(save = True)
        
        if type(today_df) == bool: 
            print("현재 사이트가 정상적으로 조회되지 않는 상황이므로 내일 다시 실행해주세요")
            
        else:
            create_csv(today_df)