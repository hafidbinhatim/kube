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
    ['COM', 'LOCALITY_PROVINCE'],
    ['COM', 'LOCALITY_DISTRICT'],
    ['COM', 'LOCALITY_SUBDISTRICT'],
    ['DIGITAL_PRODUCT', 'PRODUCT_CATEGORY_V2'],
    ['EVM', 'BRAND_MUTATION_TYPE'],
    ['ORDER', 'ORDER_DIGITAL'],
    ['PAYMENT', 'PROVIDERS_PAYMENT_METHOD'],
    ['PAYMENT', 'PAYMENT_METHOD'],
    ['PAYMENT', 'PROVIDER'],
]

for table in list_table:
    source_credential = {
        'vendor': 'mysql+pymysql',
        'host': os.environ['host'],
        'user': os.environ['user'],
        'password': os.environ['password'],
        'database': os.environ['database'],
        'port': os.environ['port'],
        'additional': 'charset=utf8mb4'
    }

    query = f""" SELECT * FROM {table[0].lower()}.{table[1].lower()}; """
    try:
        df = large_load(source_credential, query, unload_path=f's3://evm-etl/prod/{table[0]}_{table[1]}.parquet')
        initial_load_s3(snow_credential=snow_credential, df=df, schema_name=table[0], table_name=table[1],
                        load_path=f'@s3_etl/prod/{table[0]}_{table[1]}.parquet/')
    except Exception as e:
        print("Data extract error: " + str(e))

end = time.time()
print(f'Running for {round(end - start)}s')

