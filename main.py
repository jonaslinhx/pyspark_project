import sys

from lib import utils
from lib.logger import Log4J

if len(sys.argv) < 3:
    print("Usage: pyspark_proejct {local, qa, prod} {load_date} : Arguments are missing")   
    sys.exit(1)

job_run_env: str = sys.argv[1].upper()
load_date: str = sys.argv[2]

spark = utils.get_spark_session(
    env=job_run_env, 
    app_name="pyspark_project",
    log4j_file="log4j.properties",
    log_dir="logs"
)
logger = Log4J(spark)

logger.info("Spark Session Created")
logger.error("Test Error")