import pandas as pd
import os
from sqlalchemy import create_engine


def extract_mysql(credential):
    try:
        host = credential['host']
        user = credential['user']
        password = credential['password']
        database = credential['database']
        port = credential['port']

        conn_source = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4'
        print('Connecting to the database . . .')
        engine_source = create_engine(conn_source)

        extracted_df = pd.read_sql('select * from com.bank limit 5', engine_source)
        print(f'{len(extracted_df)} Rows extracted and load into Dataframe')
        return extracted_df
    except Exception as e:
        print("Data extract error: " + str(e))


source_credential = {
    'vendor': 'mysql+pymysql',
    'host': os.environ['host'],
    'user': os.environ['user'],
    'password': os.environ['password'],
    'database': os.environ['database'],
    'port': os.environ['port'],
    'additional': 'charset=utf8mb4'
}

extract_mysql(credential=source_credential)
