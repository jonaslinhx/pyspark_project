import logging
from datetime import datetime
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark_session(env:str, app_name:str, log4j_file:str, log_dir:str):
    
    logfile_name = f"{app_name}-{datetime.today().isoformat()}"
    
    if env == "LOCAL":

        master = "local[2]"
        logger.info(f"Spark Session [{env}] master: {master}")

        # -D{variable_name_for log4j.properties}
        return SparkSession.builder \
            .config('spark.driver.extraJavaOptions', f'-Dlog4j.configuration=file:{log4j_file} -Dspark.yarn.app.container.log.dir={log_dir} -Dlogfile.name={logfile_name}') \
            .appName(app_name) \
            .master(master) \
            .enableHiveSupport() \
            .getOrCreate()
    
    else:
        
        logger.info(f"Spark Session [{env}]")
        
        return SparkSession.builder \
            .appName(app_name) \
            .enableHiveSupport() \
            .getOrCreate()