import configparser
from pyspark import SparkConf

def get_config(env:str, config_path:str):
    
    config = configparser.ConfigParser()
    config.read_file(open(config_path))
    
    return {key:value for key,value in config.items(env)}

def get_spark_conf(env:str, config_path:str):

    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read_file(open(config_path))

    for key,value in config.items(env):
        spark_conf.set(key,value)

    return spark_conf

def get_data_filter(config:dict, data_filter:str):
    return "true" if config[data_filter] else config[data_filter]