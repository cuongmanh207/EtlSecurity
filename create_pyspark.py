import os
os.environ["PYSPARK_PYTHON"] = "C:/Users/PC/AppData/Local/Programs/Python/Python310/python.exe"

import logging.config
logging.config.fileConfig('Properties/Configuration/logging.config')
logger= logging.getLogger('Create_pyspark')
from pyspark.sql import SparkSession


def get_spark_object(envn,appName):
    try:
        logger.info('get_spark_object method started')
        if envn =='DEV':
            master='local'
        else:
            master='Yarn'
        logger.info('master is {}'.format(master))
        spark=SparkSession.builder.appName(appName)\
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.executor.cores", "4") \
            .config("spark.executor.instances", "5") \
            .config("spark.network.timeout", "600s") \
            .config("spark.sql.broadcastTimeout", "120000s") \
            .config("spark.driver.extraClassPath","mssql-jdbc-6.4.0.jre8.jar").getOrCreate()

    except Exception as exp:
        logging.error("An error occurred when calling main() please check the trace: %s", str(exp))
        raise
    else:
        logger.info('Spark object created...')
    return spark