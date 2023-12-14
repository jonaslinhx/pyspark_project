from typing import Literal
from lib import utils

def read_data(spark, env, enable_hive, hive_db, table_name:Literal['account','party','address'], table_schema):
    runtime_filter: str = utils.get_data_filter(env, f'{table_name}.filter')
    
    if enable_hive:
        return spark.sql(f"select * from {hive_db}.{table_name}").where(runtime_filter)
    
    else:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(table_schema) \
            .load(f"test_data/{table_name}/") \
            .where(runtime_filter)