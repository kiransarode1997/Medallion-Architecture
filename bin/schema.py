from pyspark.sql.types import *
from pyspark.sql.functions import *


# Creating schema for Order tables.
# order_id,customer_id,product_id,order_date,order_status,quantity,price
orders_schema = StructType([
    StructField("order_id",StringType(),False),
    StructField("customer_id",StringType(),False),
    StructField("product_id",StringType(),True),
    StructField("order_date",TimestampType(),True),
    StructField("order_status",StringType(),True),
    StructField("quantity",IntegerType(),True),
    StructField("price",DecimalType(),True)
])

# creating schema for customer table
# customer_id , first_name , last_name , email,quantity , phone_number
customers_schema = StructType([
    StructField("customer_id",StringType(),False),
    StructField("first_name",StringType(),True),
    StructField("last_name",StringType(),True),
    StructField("email",StringType(),True,metadata={"pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"}),
    StructField("quantity",StringType(),True),
    StructField("phone_number",StringType(),True)
])

# Creating schema for products table
products_schema = StructType([
    StructField("product_id",StringType(),False),
    StructField("product_name",StringType(),True),
    StructField("category",StringType(),True),
    StructField("price",DecimalType(),True),
    StructField("stock_quantity",IntegerType(),True)
])

# creating schema for products catalog table .
products_catalog_schema = StructType([
    StructField("product_id",StringType(),False),
    StructField("product_name",StringType(),True),
    StructField("category",StringType(),True),
    StructField("description",StringType(),True),
    StructField("price",DecimalType(),True),
    StructField("availability_status",StringType(),True)
])

# creating schema for customer review table
customer_review_schema = StructType([
    StructField("review_id",StringType(),False),
    StructField("customer_id",StringType(),False),
    StructField("product_id",StringType(),False),
    StructField("rating",IntegerType(),True),
    StructField("review_text",StringType(),True),
    StructField("review_date",TimestampType(),True)
])
