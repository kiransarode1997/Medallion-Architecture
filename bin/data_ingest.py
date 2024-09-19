from pyspark.sql.types import *
from pyspark.sql.functions import *
from schema import *

import logging.config

logging.config.fileConfig(r'F:\Data_engineer\Python\pysparkProject_medallian\config\logging_to_file.conf')
logger = logging.getLogger('data_ingest')

def create_dataframe(spark , file_path , extension , hdr , infSch ):
    try:
        if(extension == 'csv'):
            logger.info(f"Creating dataframe for {file_path} for {extension}")
            df = spark.read.option("header",hdr).option("inferSchema","True").csv(file_path)

        if(extension == 'tsv'):
            logger.info(f"Creating dataframe for {file_path} for {extension}")
            df = spark.read.option("header",hdr).option("inferSchema","True").csv(file_path,sep='\t')

        if(extension == 'json'):
            logger.info(f"Creating dataframe for {file_path} for {extension}")
            df = spark.read.option("inferSchema","True").json(file_path)
        return df

    except Exception as e :
        logger.error("Error in create dataframe", exc_info=True)
        raise
    else:
        logger.warning("Dataframes created successfully...!")