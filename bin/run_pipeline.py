import os
import sys

import environment_variables as env
from create_objects import create_spark_objects
from validations import validate_spark_object
from data_ingest import *
from data_cleaning import *
from schema import *
from data_transformation import starSchema
from pyspark.sql.functions import *

import logging
import logging.config

logging.config.fileConfig(r'F:\Data_engineer\Python\pysparkProject_medallian\config\logging_to_file.conf')
logger = logging.getLogger('root')

def main():
    try:
        logger.info("Pipeline Started")

        spark = create_spark_objects(env.envn , env.appName)
        validate_spark_object(spark)

# bronze layer data ingestion and create dataframe.
        dir_path = r'F:\Data_engineer\Python\pysparkProject_medallian\data'
        for file in os.listdir(dir_path):
            ext = file.split('.')[1]

            # creating dataframe for orders table.
            if(ext == 'csv'):
                orders_df = create_dataframe(spark,dir_path+'\\'+ file , ext , env.header ,orders_schema )
                logger.info("Dataframe create succesfully and Dataframe names is : orders_df")

            # creating dataframe for customer table.
            if(ext == 'tsv'):
                customer_df = create_dataframe(spark,r'F:\Data_engineer\Python\pysparkProject_medallian\data\sample_customers.tsv',ext , env.header,customers_schema)
                logger.info("Dataframe create succesfully and Dataframe names is : customer_df")

            # creating dataframe for product data file
            if (ext == 'tsv'):
                product_dataframe = create_dataframe(spark, r'F:\Data_engineer\Python\pysparkProject_medallian\data\sample_products.tsv', ext,env.header, products_schema)
                logger.info("Dataframe create succesfully and Dataframe names is : product_df")

            # creating dataframe for products catalog data file .
            if(ext == 'tsv'):
                product_catalog_df = create_dataframe(spark,r'F:\Data_engineer\Python\pysparkProject_medallian\data\sample_product_catalog.tsv', ext , env.header ,products_catalog_schema)
                logger.info("Dataframe create succesfully and Dataframe names is : product_catalog_df")

            # creating dataframe for customer review table .
            if(ext == 'json'):
                customer_review_df = create_dataframe(spark,dir_path+'\\'+file , ext , env.header,customer_review_schema)
                logger.info("Dataframe create succesfully and Dataframe names is : customer_review_df")

        customer_clean = cleaning_customers(customer_df)
        customer_review_clean = cleaning_customers_review(customer_review_df)
        order_clean = cleaning_orders(orders_df)
        product_clean = cleaning_products(product_dataframe)
        product_catalog_clean = cleaning_products_catalog(product_catalog_df)

# Silver Layer ( data cleaning and enrichment)
        clean_order = cleaning_orders(order_clean)
        #clean_order.show(truncate= False)
        # Final Order : order_id,customer_id,product_id,order_date,order_status,quantity,price,total_amount

        enrichment_customer = cleaning_customers(customer_clean,clean_order)


        #enrichment_customer.show(truncate= False)
        # Final customer : customer_id,full_name,email,address,phone_number,total_orders

        enrich_production = cleaning_products(product_clean ,product_catalog_df,customer_review_df)
        #enrich_production.show(truncate= False)
        # Final Product : product_id,product_name,category,price,stock_quantity,description,availability_status,average_rating


# Gold Layer.
        star = starSchema(enrichment_customer,enrich_production,clean_order)
        star.show(truncate = False)

        #star.createOrReplaceTempView("sale")
        # output : order_id,customer_id,product_id,order_date,quantity,total_amount


        #visualize(star)


    except Exception as e:
        logger.error("Error in main , please check details" +str(e))
        sys.exit(1)

if __name__ == '__main__':
    main()