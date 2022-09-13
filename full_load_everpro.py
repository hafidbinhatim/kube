from utils.snowhail import large_load, initial_load_s3
import time
import os

start = time.time()

snow_credential = {
    'snow_user': os.environ['snow_user'],
    'snow_password': os.environ['snow_password'],
    'snow_account': os.environ['snow_account'],
    'snow_db': os.environ['snow_db'],
    'snow_schema': os.environ['snow_schema'],
    'snow_wh': os.environ['snow_wh'],
    'snow_role': os.environ['snow_role']
}

list_table = [
#     ['POPAKET_LOGISTIC', 'LOGISTICS'],
    ['POPAKET_LOGISTIC', 'LOCATION_CITIES'],
#     ['POPAKET_LOGISTIC', 'LOCATION_DISTRICTS'],
#     ['POPAKET_LOGISTIC', 'LOCATION_PROVINCES'],
#     ['POPAKET_LOGISTIC', 'LOCATION_SUB_DISTRICTS'],
#     ['POPAKET_ORDER', 'LOGISTICS'],
#     ['POPAKET_ORDER', 'LOCATION_CITIES'],
#     ['POPAKET_ORDER', 'LOCATION_DISTRICTS'],
#     ['POPAKET_ORDER', 'LOCATION_PROVINCES'],
#     ['POPAKET_ORDER', 'LOCATION_SUB_DISTRICTS'],
#     ['POPAKET_ORDER', 'LOGISTIC_ORDER_STATUS'],
#     ['POPAKET_WALLET', 'MUTATION_TRANSLATIONS'],
#     ['POPAKET_WALLET', 'MUTATION_TYPES'],
#     ['POPAKET_WALLET', 'TRANSACTION_STATUS_TRANSLATIONS'],
#     ['POPAKET_WALLET', 'TRANSACTION_TYPES']
]

for table in list_table:
    source_credential = {
        'vendor': 'postgresql',
        'host': os.environ['host'],
        'user': os.environ['user'],
        'password': os.environ['password'],
        'database': table[0].lower(),
        'port': os.environ['port'],
        'additional': 'sslmode=require'
    }
    pg_schema = 'public'

    query = f""" SELECT * FROM {pg_schema}.{table[1].lower()}; """
    try:
        df = large_load(source_credential, query, unload_path=f's3://evm-etl/prod/{table[0]}_{table[1]}.parquet')
        initial_load_s3(snow_credential=snow_credential, df=df, schema_name=table[0], table_name=table[1],
                        load_path=f'@s3_etl/prod/{table[0]}_{table[1]}.parquet/')
    except Exception as e:
        print("Data extract error: " + str(e))
end = time.time()
print(f'Running for {round(end - start)}s')
