# # # Gold Layer
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging.config
logging.config.fileConfig(r'F:\Data_engineer\Python\pysparkProject_medallian\config\logging_to_file.conf')
logger = logging.getLogger('data_transformation')
#
# # Final Order : order_id,customer_id,product_id,order_date,order_status,quantity,price,total_amount
# # Final customer : customer_id,full_name,email,address,phone_number,total_orders
# # Final Product : product_id,product_name,category,price,stock_quantity,description,availability_status,average_rating
def starSchema(customer,product,order):
    try:
        logger.info("Star Schema creation started...!")

        # Customer Dimension Table.
        customer_dim = customer.select(col("customer_id"),col("full_name"),col("email"),col("address"),col("phone_number")).dropDuplicates(["customer_id"])

        # Product Dimension Table.
        product_dim = product.select(col("product_id"),col("product_name"),col("category"),col("price").cast(DecimalType(10,2)),col("stock_quantity").cast("int"),col("average_rating").cast(DecimalType(3,2))).dropDuplicates(["product_id"])

        # Time Dimension table.
        time_dim = order.select(col("order_date").alias("date_key"),
                                dayofmonth("order_date").alias("day"),
                                month("order_date").alias("month"),
                                year("order_date").alias("year"),
                                quarter("order_date").alias("quarter"),
                                date_format(col("order_date"),"EEEE").alias("day_of_week")).dropDuplicates(["date_key"])
        # Fact Table.
        sale_df = order.select(col("order_id").cast("string"),
                               col("customer_id").cast("string"),
                               col("product_id").cast("string"),
                               to_timestamp(col("order_date")).alias("order_date"),
                               col("quantity").cast("int"),
                               col("total_amount")).dropDuplicates(["order_id"])
        #
        # # Optionally, validate foreign keys by joining with dimension tables
        # # For example, ensure customer_id exists in customers_dim
        sales_fact = sale_df.join(customer_dim, on="customer_id", how="inner")
        sales_fact = sales_fact.join(product_dim, on="product_id", how="inner")
        sales_fact = sales_fact.join(time_dim, sales_fact.order_date == time_dim.date_key, how="inner").drop(time_dim.date_key)

        sales_fact = sales_fact.select(
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "quantity",
            "total_amount"
        )
        logger.info("Star Schema created successfully...!")
        return sales_fact

    except Exception as e:
        logger.error("Error in Gold Layer data from data_transformation.py script",exc_info=True)
        raise