import sys

from lib import utils
from lib.logger import Log4J
from typing import List
from pprint import pformat

if len(sys.argv) < 3:
    print("Usage: pyspark_proejct {local, qa, prod} {load_date} : Arguments are missing")   
    sys.exit(1)

job_run_env: str = sys.argv[1].upper()
load_date: str = sys.argv[2]

spark = utils.get_spark_session(
    env=job_run_env, 
    conf_files=["conf/pyspark_project.conf", "conf/spark.conf"]
    # app_name="pyspark_project",
    # log4j_file="log4j.properties",
    # log_dir="logs"
)
logger = Log4J(spark)

sc = spark.sparkContext
spark_config: List[dict] = sc.getConf().getAll()

logger.info("\n\n")
logger.info(100*'*')
logger.info("Spark Session Created")
logger.info(f"Spark Config: {pformat(spark_config)}")
logger.info("Spark End")
logger.info(f"{100*'*'}\n\n")