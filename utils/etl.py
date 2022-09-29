# import needed libraries
from sqlalchemy import create_engine
import pandas as pd
import time

def execute_pg(credential, query):
    try:
        host = credential['host']
        user = credential['user']
        password = credential['password']
        database = credential['database']
        port = credential['port']

        conn_source = f'postgresql://{user}:{password}@{host}:{port}/{database}'
        print('Connecting to the database . . .')
        engine_source = create_engine(conn_source)
        conn = engine_source.connect()
        conn.execute(query)
        print('Query Executed Successfully')

    except Exception as e:
        print("Query Failed : " + str(e))

def extract_pg(credential, query):
    try:
        host = credential['host']
        user = credential['user']
        password = credential['password']
        database = credential['database']
        port = credential['port']

        conn_source = f'postgresql://{user}:{password}@{host}:{port}/{database}'
        print('Connecting to the database . . .')
        engine_source = create_engine(conn_source)
        query = query

        extracted_df = pd.read_sql(query, engine_source)
        print(f'{len(extracted_df)} Rows extracted and load into Dataframe')
        return extracted_df
    except Exception as e:
        print("Data extract error: " + str(e))


def extract_mysql(credential, query):
    try:
        host = credential['host']
        user = credential['user']
        password = credential['password']
        database = credential['database']
        port = credential['port']

        conn_source = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4'
        print('Connecting to the database . . .')
        engine_source = create_engine(conn_source)
        query = query

        extracted_df = pd.read_sql(query, engine_source)
        print(f'{len(extracted_df)} Rows extracted and load into Dataframe')
        return extracted_df
    except Exception as e:
        print("Data extract error: " + str(e))


def insert(credential, df2load,schema_name, table_name, method='append'):
    try:
        host_r = credential['host']
        user_r = credential['user']
        password_r = credential['password']
        database_r = credential['database']
        port_r = credential['port']

        print(f'Loading {len(df2load)} rows to target')
        conn_target = f'postgresql://{user_r}:{password_r}@{host_r}:{port_r}/{database_r}'
        engine_target = create_engine(conn_target)
        # save df to postgres

        df2load.to_sql(table_name, engine_target, schema=schema_name, if_exists=method, index=False, method='multi',
                       chunksize=10000)
        # add elapsed time to final print out
        print("Data loaded successful")
    except Exception as e:
        print("Data load error: " + str(e)[0:1000])


def upsert(credential, df2load, schema_name, temp_schema_name, table_name, left_id='id', right_id='id'):
    try:
        host_r = credential['host']
        user_r = credential['user']
        password_r = credential['password']
        database_r = credential['database']
        port_r = credential['port']

        conn_target = f'postgresql://{user_r}:{password_r}@{host_r}:{port_r}/{database_r}'
        engine_target = create_engine(conn_target)
        create_temp = f"""create table if not exists {temp_schema_name}.{table_name} (like {schema_name}.{table_name})"""
        delete_query = f"""delete from {schema_name}.{table_name} 
        where {left_id} in (select {right_id} from {temp_schema_name}.{table_name})"""

        insert_query = f"""insert into {schema_name}.{table_name} select *
        from {temp_schema_name}.{table_name}"""

        # save df to postgres
        print(f'Loading {len(df2load)} rows to temporary table')
        conn = engine_target.connect()
        conn.execute(create_temp)
        conn.execute(f'truncate table {temp_schema_name}.{table_name}')

        df2load.to_sql(table_name, engine_target, schema=temp_schema_name, if_exists='append', index=False,
                       method='multi', chunksize=10000)

        conn.execute(delete_query)
        print('Upsert temp table to target')
        conn.execute(insert_query)
        print("Data loaded successful")

    except Exception as e:
        print("Data load error: " + str(e)[0:1000])


def large_load(credential, query, table_name, schema_name, method='append'):
    try:
        host = credential['host']
        user = credential['user']
        password = credential['password']
        database = credential['database']
        port = credential['port']

        conn_source = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4'
        print('Connecting to the database . . .')
        engine_source = create_engine(conn_source).execution_options(stream_results=True)
        query = query

        extracted_df = pd.read_sql(query, engine_source, chunksize=100000)
        start = time.time()
        for dataset in extracted_df:
            print(f'{len(dataset)} Rows extracted and load into Dataframe')
            conn_target = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
            engine_target = create_engine(conn_target)
            # save df to postgres

            dataset.to_sql(table_name, engine_target, schema=schema_name, if_exists=method, index=False, method='multi',
                           chunksize=100000)

            print("Data loaded successful")
        end = time.time()
        print(f'Running for {round(end - start)}s')
    except Exception as e:
        print("Data extract error: " + str(e))
