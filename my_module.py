from sqlalchemy import create_engine
import mysql_info

MYSQL_HOST = mysql_info.host
MYSQL_USER = mysql_info.user
MYSQL_PW = mysql_info.password

def get_connection(host = MYSQL_HOST,
                user = MYSQL_USER,
                password = MYSQL_PW,
                db = None):
    """sqlalchemy을 이용해 create_engine.connect() 객체를 반환"""
    if db == None:
        db_connection_str = f'mysql+pymysql://{user}:{password}@{host}/'
    else:
        db_connection_str = f'mysql+pymysql://{user}:{password}@{host}/{db}'
    
    engine = create_engine(db_connection_str, encoding = 'utf-8')
    conn = engine.connect()
    
    return conn