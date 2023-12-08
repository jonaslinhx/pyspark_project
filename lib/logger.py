class Log4J:

    def __init__(self, spark):
        """
        INPUT:
            spark: SparkSession object
        """
        log4j = spark._jvm.org.apache.log4j 
        conf = spark.sparkContext.getConf() # NOTE: Get access to the $SPARK_HOME/conf/*.conf used for this application
        app_name = conf.get("spark.app.name")
        self.logger = log4j.LogManager.getLogger(app_name)

    def warn(self, message:str):
        self.logger.warn(message)

    def info(self, message:str):
        self.logger.info(message)

    def error(self, message:str):
        self.logger.error(message)

    def debug(self, message:str):
        self.logger.debug(message)