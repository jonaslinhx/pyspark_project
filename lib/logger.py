class Log4J:

    def __init__(self, spark):
        """
        INPUT:
            spark: SparkSession object
        """
        log4j = spark._jvm.org.apache.log4j 
        self.logger = log4j.LogManager.getLogger("pyspark_project") # follow log4j.properties

    def warn(self, message:str):
        self.logger.warn(message)

    def info(self, message:str):
        self.logger.info(message)

    def error(self, message:str):
        self.logger.error(message)

    def debug(self, message:str):
        self.logger.debug(message)