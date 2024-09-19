from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging.config
logging.config.fileConfig(r'C:\Users\Hp\PycharmProjects\E-commerce\config\logging_to_file.conf')
logger = logging.getLogger('data_cleaning')
def cleaning_customers(df):
    try:
        logger.info("Data cleaning of customer dataframe started")
        # customer_id,first_name,last_name,email,address,phone_number---- customer dataframe fields
        df_cleaned = df.dropDuplicates(['customer_id']).filter(length(col("phone_number")) >= 10)

        # selecting required column.
        clean_customer = df_cleaned.select("customer_id","first_name","last_name","email","address","phone_number")
        logger.info("Data cleaning of Customer data is sucessfully Done.")
        return clean_customer
    except Exception as e:
        logger.error("Error in Data_cleaning.py of customer data ",exc_info=True)
        raise

def cleaning_customers_review(df):
    try:
        logger.info("Data cleaning of customers_review dataframe started")
        # review_id , customer_id,product_id,rating,review_text,review_date
        df_cleaned = df.na.drop(subset=['review_id', 'customer_id', 'product_id', 'rating'])
        df_clean = df_cleaned.withColumn("rating",col("rating").cast(IntegerType())).filter((col("rating") >= 1) & (col("rating") <= 5))
        # selecting required column.
        clean_customer_review = df_clean.select("review_id","customer_id","product_id","rating","review_text","review_date")
        logger.info("Data cleaning of customers_review dataframe is sucessfully Done.")
        return clean_customer_review

    except Exception as e:
        logger.error("Error in Data_cleaning.py customers_review data from data_cleaning.py script",exc_info=True)
        raise

def cleaning_orders(df):
    try:
        logger.info("Data cleaning of orders dataframe started")
        # order_id,customer_id,product_id,order_date,order_status,quantity,price
        orders_cleaned = df.dropDuplicates(['order_id'])

        # Drop rows with missing essential fields (order_id, customer_id, product_id)
        orders_cleaned = orders_cleaned.dropna(subset=['order_id', 'customer_id', 'product_id'])

        # Fill missing values in 'order_status', 'quantity', and 'price' (if needed)
        orders_cleaned = orders_cleaned.fillna({
            'order_status': 'unknown',  # Fill missing order_status with 'unknown'
            'quantity': 0,  # Fill missing quantity with 1
            'price': 0.0  # Fill missing price with 0.0
        })
        # selecting required column.
        clean_order_df = orders_cleaned.select("order_id","customer_id","product_id","order_date","order_status","quantity","price")
        logger.info("Data cleaning of orders data is sucessfully Done.")
        return clean_order_df

    except Exception as e:
        logger.error("Error in Data_cleaning.py orders dataframe",exc_info=True)
        raise

def cleaning_products(df):
    try:
        logger.info("Data cleaning of products dataframe started")
        # # product_id,product_name,category,price,stock_quantity
        df_clean = df.na.fill({
            'product_name': 'unknown',
            'category': 'misssing',
            'price': 0.0,
            'stock_quantity':0
        })
        prod_df = df_clean.filter((col("price")>=0) & (col("stock_quantity")>=0))
        # selecting required column.
        clean_product_df = prod_df.select("product_id","product_name","category","price","stock_quantity")
        logger.info("Data cleaning of products data is sucessfully Done.")
        return clean_product_df

    except Exception as e:
        logger.error("Error in Data_cleaning.py from products dataframe",exc_info=True)
        raise

def cleaning_products_catalog(df):
    try:
        logger.info("Data cleaning of products_catalog dataframe started")
        # product_id , product_name , category,description,price,availability_status
        df_clean = df.na.fill({
            'product_name': 'unknown',
            'category': 'misssing',
            'description': 'No description available',
            'price': 0.0,
            'availability_status': 'unavailable'
        })
        valid_status = ["In Stock", "Out of Stock"]
        prod_df = df_clean.withColumn("availability_status", when(col("availability_status").isin(valid_status),col("availability_status")).otherwise("unavailable")).dropDuplicates(["product_id"])

        # selecting required column.
        clean_prod_cat = prod_df.select("product_id","product_name","category","description","price","availability_status")
        logger.info("Data cleaning of products_catalog data is sucessfully Done.")
        return clean_prod_cat

    except Exception as e:
        logger.error("Error in Data_cleaning.py products_catalog dataframe",exc_info=True)
        raise