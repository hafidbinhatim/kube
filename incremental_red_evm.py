import time
from utils.etl import upsert, extract_mysql
import os
import ast

start = time.time()

source_credential = {
    'host': os.environ['host'],
    'user': os.environ['user'],
    'password': os.environ['password'],
    'database': os.environ['database'],
    'port': os.environ['port'],
}

red_credential = {
    'host': os.environ['red_host'],
    'user': os.environ['red_user'],
    'password': os.environ['red_password'],
    'database': os.environ['red_database'],
    'port': os.environ['red_port'],
}

list = os.environ.get("list_table")
list_table = ast.literal_eval(list)

ds = '2022-09-28'

for table in list_table:
    date_column = table[3]
    query = f""" SELECT * from {table[0].lower()}.{table[1].lower()} 
            where DATE({date_column}) = '{ds}'; """
    print(query)
    try:
        df = extract_mysql(source_credential, query)
        # Transformation Here
        temp_schema = 'temp_etl'
        print('extract done')
        upsert(credential=red_credential, df2load=df, table_name=table[1], schema_name=table[0],
               temp_schema_name=temp_schema, left_id=table[2], right_id=table[2])

    except Exception as e:
        print("Data extract error: " + str(e))

end = time.time()
print(f'Running for {round(end - start)}s')
