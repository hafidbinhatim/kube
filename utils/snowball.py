import pandas as pd
import snowflake.connector as snow
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import awswrangler as wr


def extract_mysql(credential, query):
    try:
        vendor = credential['vendor']
        host = credential['host']
        user = credential['user']
        password = credential['password']
        database = credential['database']
        port = credential['port']
        additional = credential['additional']

        conn_source = f'{vendor}://{user}:{password}@{host}:{port}/{database}?{additional}'
        print('Connecting to the database . . .')
        engine_source = create_engine(conn_source)
        query = query

        extracted_df = pd.read_sql(query, engine_source)
        print(f'{len(extracted_df)} Rows extracted and load into Dataframe')
        return extracted_df
    except Exception as e:
        print("Data extract error: " + str(e))


def to_s3(snow_credential, df, table_name, temp_schema_name, unload_path, load_path):
    df.columns = df.columns.str.upper()
    if not df.empty:
        wr.s3.to_parquet(
            df=df,
            path=unload_path,
            index=False,
            dataset=True,
            mode="overwrite",
            # partition_cols=['YEAR']
        )

    snow_user = snow_credential['snow_user']
    snow_pass = snow_credential['snow_password']
    snow_account = snow_credential['snow_account']
    snow_db = snow_credential['snow_db']
    snow_schema = snow_credential['snow_schema']
    snow_wh = snow_credential['snow_wh']
    snow_role = snow_credential['snow_role']

    conn = snow.connect(user=snow_user, password=snow_pass, account=snow_account, database=snow_db, schema=snow_schema,
                        warehouse=snow_wh, role=snow_role)

    print(f'Loading {len(df)} rows to {table_name}')
    cols = list(df.columns)
    curate_cols = ','.join([str(n) for n in ['$1:' + s for s in cols]])
    query_clear = f"""TRUNCATE TABLE {temp_schema_name}.{table_name};"""
    query_load = f"""COPY INTO {temp_schema_name}.{table_name} FROM (
            SELECT {curate_cols}
            FROM {load_path})
            FILE_FORMAT = (TYPE = 'PARQUET'); """
    print(query_load)
    cursor = conn.cursor()
    cursor.execute(query_clear)
    cursor.execute(query_load)
    print('Data Load to Temp Table Finished')


def upsert(snow_credential, schema_name, temp_schema_name, table_name, temp_table_name, left_id='id', right_id='id'):
    try:
        snow_user = snow_credential['snow_user']
        snow_pass = snow_credential['snow_password']
        snow_account = snow_credential['snow_account']
        snow_db = snow_credential['snow_db']
        snow_schema = snow_credential['snow_schema']
        snow_wh = snow_credential['snow_wh']
        snow_role = snow_credential['snow_role']

        conn = snow.connect(user=snow_user, password=snow_pass, account=snow_account, database=snow_db,
                            schema=snow_schema, warehouse=snow_wh, role=snow_role)
        cursor = conn.cursor()
        delete_query = f"""delete from "{schema_name}"."{table_name}" 
        where {left_id} in (select {right_id} from {temp_schema_name}.{temp_table_name})"""

        insert_query = f"""insert into "{schema_name}"."{table_name}" select *
        from {temp_schema_name}.{temp_table_name} """
        # save df to postgres
        print(f'Loading rows to temporary table')
        cursor.execute(delete_query)
        print('Upsert temp table to target')
        cursor.execute(insert_query)
        print("Data loaded successful")

    except Exception as e:
        print("Data load error: " + str(e)[0:1000])


def get_column_mysql(snow_credential, schema, table):
    snow_user = snow_credential['snow_user']
    snow_pass = snow_credential['snow_password']
    snow_account = snow_credential['snow_account']
    snow_db = snow_credential['snow_db']
    snow_schema = snow_credential['snow_schema']
    snow_wh = snow_credential['snow_wh']
    snow_role = snow_credential['snow_role']

    url = URL(user=snow_user, password=snow_pass, account=snow_account, database=snow_db, schema=snow_schema,
              warehouse=snow_wh, role=snow_role)

    engine = create_engine(url)
    connection = engine.connect()
    query = f"""select * from "{schema}"."{table}" limit 1"""
    df = pd.read_sql(query, connection)
    cols = list(df.columns)
    curate_cols = ','.join([str(n) for n in ['`'+s+'`' for s in cols]])
    return curate_cols


def get_column_pg(snow_credential, schema, table):
    snow_user = snow_credential['snow_user']
    snow_pass = snow_credential['snow_password']
    snow_account = snow_credential['snow_account']
    snow_db = snow_credential['snow_db']
    snow_schema = snow_credential['snow_schema']
    snow_wh = snow_credential['snow_wh']
    snow_role = snow_credential['snow_role']

    url = URL(user=snow_user, password=snow_pass, account=snow_account, database=snow_db, schema=snow_schema,
              warehouse=snow_wh, role=snow_role)

    engine = create_engine(url)
    connection = engine.connect()
    query = f"""select * from "{schema}"."{table}" limit 1"""
    df = pd.read_sql(query, connection)
    cols = list(df.columns)
    curate_cols = ','.join([str(n) for n in [s for s in cols]])
    return curate_cols
