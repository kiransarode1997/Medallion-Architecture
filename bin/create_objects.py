import logging.config
from pyspark.sql import SparkSession
from environment_variables import *

logging.config.fileConfig(r'F:\Data_engineer\Python\pysparkProject_medallian\config\logging_to_file.conf')
logger = logging.getLogger('create_objects')


def create_spark_objects(envn, appName):
    try:
        logger.info("Spark Object Creation started")
        if(envn == 'TEST'):
            master = 'local'
        else:
            master = 'yarn'

        spark = SparkSession.builder.appName(appName).master(master).getOrCreate()
        return spark
    except Exception as e:
        logger.error("Error in create_spark_objects() from create_objects.py script " + str(e))
    else:
        logger.warning("Spark object createtion successful")
