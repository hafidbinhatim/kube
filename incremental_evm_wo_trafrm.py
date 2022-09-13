import time
import os
import ast
from utils.snowball import upsert, to_s3, extract_mysql, get_column_mysql

start = time.time()

source_credential = {
    'vendor': 'mysql+pymysql',
    'host': os.environ['host'],
    'user': os.environ['user'],
    'password': os.environ['password'],
    'database': os.environ['database'],
    'port': os.environ['port'],
    'additional': 'charset=utf8mb4'
}

snow_credential = {
    'snow_user': os.environ['snow_user'],
    'snow_password': os.environ['snow_password'],
    'snow_account': os.environ['snow_account'],
    'snow_db': os.environ['snow_db'],
    'snow_schema': os.environ['snow_schema'],
    'snow_wh': os.environ['snow_wh'],
    'snow_role': os.environ['snow_role']
}

list = os.environ.get("list_table")
list_table = ast.literal_eval(list)

ds = os.environ['ds']

for table in list_table:
    date_column = table[3]
    list_column = get_column_mysql(snow_credential=snow_credential, table=table[1], schema=table[0])
    query = f""" SELECT {list_column} from {table[0].lower()}.{table[1].lower()} 
            where DATE({date_column}) = '{ds}'; """
    print(query)
    try:
        df = extract_mysql(source_credential, query)
        # Transformation Here
        if table[1] == 'BRAND_PIC':
            df['removedtime'] = df['removedtime'].replace('0000-00-00 00:00:00', None)

        temp_table = f'TEMP_{table[0]}_{table[1]}'
        temp_schema = 'TEMP_ETL'
        print('extract done')
        to_s3(snow_credential=snow_credential, df=df, temp_schema_name=temp_schema, table_name=temp_table,
              unload_path=f's3://evm-etl/prod/TEMP_{table[0]}_{table[1]}.parquet',
              load_path=f'@s3_etl/prod/TEMP_{table[0]}_{table[1]}.parquet/')
        upsert(snow_credential=snow_credential, schema_name=table[0], temp_schema_name=temp_schema,
               table_name=table[1], temp_table_name=temp_table, left_id=table[2], right_id=table[2])
    except Exception as e:
        print("Data extract error: " + str(e))

end = time.time()
print(f'Running for {round(end - start)}s')
