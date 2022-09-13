import pandas as pd
import snowflake.connector as snow
from sqlalchemy import create_engine
import awswrangler as wr


def unload_to_s3(df, unload_path):
    df.columns = df.columns.str.upper()
    wr.s3.to_parquet(
        df=df,
        path=unload_path,
        index=False,
        dataset=True,
        mode="append"
    )


def large_load(credential, query, unload_path):
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
        engine_source = create_engine(conn_source).execution_options(stream_results=True)
        extracted_df = pd.read_sql(query, engine_source, chunksize=50000)
        wr.s3.delete_objects(unload_path)

        for dataset in extracted_df:
            print(f'{len(dataset)} Rows extracted and load into Dataframe')
            dataset.columns = dataset.columns.str.upper()

            # convert time
            date_columns = dataset.select_dtypes(include=['timedelta64']).columns.tolist()
            dataset[date_columns] = dataset[date_columns].astype(str)
            dataset[date_columns] = dataset[date_columns].replace(r"\d+ days ([\d:]+)", r"\1", regex=True)
            dataset[date_columns] = dataset[date_columns].replace("NaT", None)
            # convert UUID
            to_str = dataset.select_dtypes(include=['object']).columns.tolist()
            dataset[to_str] = dataset[to_str].astype(str)
            dataset[to_str] = dataset[to_str].replace("None", None)

            print(f'Loading {len(dataset)} rows to S3')
            unload_to_s3(df=dataset, unload_path=unload_path)
            print("Data Unloaded to S3 successful")
        return list(dataset.columns)

    except Exception as e:
        print("Data extract error: " + str(e))


def initial_load_s3(snow_credential, df, table_name, schema_name, load_path):
    snow_user = snow_credential['snow_user']
    snow_password = snow_credential['snow_password']
    snow_account = snow_credential['snow_account']
    snow_db = snow_credential['snow_db']
    snow_schema = snow_credential['snow_schema']
    snow_wh = snow_credential['snow_wh']
    snow_role = snow_credential['snow_role']

    conn = snow.connect(user=snow_user, password=snow_password, account=snow_account, database=snow_db, schema=snow_schema,
                        warehouse=snow_wh, role=snow_role)

    print(f'Loading rows to {table_name}')
    cols = df
    curate_cols = ','.join([str(n) for n in ['$1:' + s for s in cols]])
    create_temp = f"""CREATE OR REPLACE TABLE TEMP_ETL.TEMP_{schema_name}_{table_name} CLONE "{schema_name}"."{table_name}";"""
    query_clear = f"""TRUNCATE TABLE TEMP_ETL.TEMP_{schema_name}_{table_name};"""

    query_load = f"""COPY INTO TEMP_ETL.TEMP_{schema_name}_{table_name} FROM (
            SELECT {curate_cols}
            FROM {load_path})
            FILE_FORMAT = (TYPE = 'PARQUET'); """
    switch_out = f"""ALTER TABLE "{schema_name}"."{table_name}" RENAME TO TEMP_ETL.PREV_{schema_name}_{table_name}; """
    switch_in = f"""ALTER TABLE TEMP_ETL.TEMP_{schema_name}_{table_name} RENAME TO "{schema_name}"."{table_name}"; """
    drop_prev = f"""DROP TABLE TEMP_ETL.PREV_{schema_name}_{table_name}"""
    print(query_load)
    cursor = conn.cursor()
    try:
        cursor.execute(create_temp)
        cursor.execute(query_clear)
        print('Create temp table')
        cursor.execute(query_load)
        print('Load data to temp table')
        cursor.execute(drop_prev)
        cursor.execute(switch_out)
        print('Switch out table to temp')
        cursor.execute(switch_in)
        print('Switch in loaded table')
        print('Data Load Finished')
    except Exception as e:
        print("Data load error: " + str(e))


