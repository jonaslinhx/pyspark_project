import logging
import configparser

from typing import List
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_config(config_files:list, section:str) -> List[dict]:

        spark_conf = SparkConf()

        for config_file in config_files:

            config = configparser.ConfigParser()
            config.read_file(open(config_file))

            for key,value in config.items(section):
                 
                 spark_conf.set(key, value)

        return spark_conf


def get_spark_session(env:str, conf_files:list): # log4j_file:str, log_dir:str, 
    
    spark_conf = get_config(conf_files, env)

    logger.info(spark_conf.getAll())

    return SparkSession.builder\
        .config(conf=spark_conf).enableHiveSupport().getOrCreate()