import sys
import uuid

from typing import List, Union
from pprint import pformat
from pyspark.sql.functions import col, struct, to_json


from lib import utils
from lib.logger import Log4J
from lib import dataloader, schemas, transformations, configloader

if len(sys.argv) < 3:
    print("Usage: pyspark_proejct {local, qa, prod} {load_date} : Arguments are missing")   
    sys.exit(1)

job_run_env: str = sys.argv[1].upper()
load_date: str = sys.argv[2]
job_run_id: str = f"pyspark_project-{str(uuid.uuid4())}"
conf = configloader.get_config(job_run_env, "conf/pyspark_project.conf")
enable_hive: bool = True if conf["enable.hive"] == "true" else False 
hive_db: Union[str,None] = conf['hive.database']

# Create Session
spark = utils.get_spark_session(
    env=job_run_env, 
    spark_config_path= "conf/spark.conf"
)
logger = Log4J(spark)

logger.info("Reading Account DF")
accounts_df = dataloader.read_data(spark, job_run_env, enable_hive, hive_db, "account", schemas.account_schema)
contract_df = transformations.get_contract(accounts_df)

logger.info("Reading Party DF")
party_df = dataloader.read_data(spark, job_run_env, enable_hive, hive_db, "party", schemas.party_schema)
relation_df = transformations.get_relations(party_df)

logger.info("Reading Address DF")
address_df = dataloader.read_data(spark, job_run_env, enable_hive, hive_db, "address", schemas.address_schema)
relaton_address_df = transformations.get_address(address_df)

logger.info("Join Party Relatons and Address")
party_address_df = transformations.join_party_address(relation_df, relaton_address_df)

logger.info("Join Account and Parties")
data_df = transformations.join_contract_party(contract_df, party_address_df)

logger.info("Apply Header and create Event")
final_df = transformations.apply_header(spark, data_df)
logger.info("Preparing to send data to Kafka")

# Kafka only accepts key-value dataframe
kafka_kv_df = final_df.select(
    col("payload.contractIdentifier.newValue").alias("key"),
    to_json(struct("*")).alias("value")
)

# kafka_kv_df.write.format("kafka").mode("overwrite").save("test_data/noop")

# Keep in vault or some secure place, authorise application to extract it from there
api_key = conf['kafka.api_key']
api_secret = conf['kafka.api_secret']

kafka_kv_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.srvers", conf['kafka.bootstrap.srvers']) \
    .option("topic", conf["kafka.topic"]) \
    .option("kafka.security.protocol", conf["kafka.security.protocol"]) \
    .option("kafka.sasl.jaas.config", conf["kafka.sasl.jaas.config"].format(api_key,api_secret)) \
    .option("kafka.sasl.mechanism", conf["kafka.sasl.mechanism"]) \
    .option("kafka.client.dns.lookup", conf["kafka.client.dns.lookup"]) \
    .save()

logger.info("Finished sending data to Kafka")


# sc = spark.sparkContext
# spark_config: List[dict] = sc.getConf().getAll()

# logger.info("\n\n")
# logger.info(100*'*')
# logger.info("Spark Session Created")
# logger.info(f"Spark Config: {pformat(spark_config)}")
# logger.info("Spark End")
# logger.info(f"{100*'*'}\n\n")