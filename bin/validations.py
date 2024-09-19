import logging
import logging.config

logging.config.fileConfig(r'C:\Users\Hp\PycharmProjects\E-commerce\config\logging_to_file.conf')
logger = logging.getLogger('validations')


def validate_spark_object(spark_obj):
    try:
        logger.info('validate_spark_object() started')
        date_df = spark_obj.sql("""select current_date""")
        print(f"current date is {str(date_df.collect())}")
        logger.info('validate_saprk_object() ended')
    except NameError as e:
        logger.error("Name Error in validate_spark_object() from validations.py" , exc_info=True)
        raise
    except Exception as e :
        logger.error("Unknown Error in validate_spark_object() from valiations.py script ", exc_info=True)
        raise
    else:
        logger.warning("Spark object validation success")

def validate_df(df):
    try:
        logger.info(f"Validating DF  by printing first 10 records," + str(df.take(10)))
        logger.info(f"Validating DF  by printing it's schema, " + str(df.schema))
    except Exception as e :
        logger.error()
        raise
