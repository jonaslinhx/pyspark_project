import logging
import configparser

from typing import List
from datetime import datetime
from pyspark.sql import SparkSession

from lib import configloader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session(env:str, spark_config_path:str): # log4j_file:str, log_dir:str, 
    
    spark_conf = configloader.get_spark_conf(env, spark_config_path)

    logger.info(spark_conf.getAll())

    return SparkSession.builder\
            .config(conf=spark_conf)\
            .enableHiveSupport()\
            .getOrCreate()