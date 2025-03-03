
"""
Online sample 
Example - sales dataset containing transaction details. 
The goal is to:
	1. Filter data to include only recent transactions.
	2. Enrich the dataset by categorizing products based on price.
	3. Compute running totals and ranking per customer using window functions.
	4. Aggregate total sales per product category.
Pivot the data to analyze sales trends over time.

"""

from pyspark.sql.functions import col, dense_rank, when, avg, count
from pyspark.sql.window import Window
from src.utils.helpers import SparkSessionManager

# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType , DateType

# Initialize Spark session
spark = SparkSessionManager.get_spark_session()

# Sample data
data = [
    (1, 101, "Laptop", "Electronics", 1000, 1, "2024-02-20"),
    (2, 102, "Phone", "Electronics", 800, 2, "2024-02-21"),
    (3, 101, "Shoes", "Fashion", 150, 3, "2024-02-22"),
    (4, 103, "Shirt", "Fashion", 50, 4, "2024-02-23"),
    (5, 102, "Laptop", "Electronics", 1200, 1, "2024-02-24"),
]

columns = ["transaction_id", "customer_id", "product", "category", "price", "quantity", "transaction_date"]

# Define Schema explicitly. 
# Better to read it as a string and then do a source cleanup
# columns = StructType([
#     StructField("transaction_id", IntegerType(), True),
#     StructField("customer_id", IntegerType(), True),
#     StructField("product", StringType(), True),
#     StructField("category", StringType(), True),
#     StructField("price", DoubleType(), True),
#     StructField("quantity", IntegerType(), True),
#     StructField("transaction_date", DateType(), True)  # can be a timestamp type. There maynot be one for datetime 
# ])
# Create DataFrame

sales_df = spark.createDataFrame(data, columns)
sales_df.printSchema()
sales_df.show()


# TODO: Cleanse the source 


# Filter recent transactions 
recent_sales_df = sales_df.filter(col("transaction_date") >= "2024-02-20")

#Categorize products based on price
categorized_df = recent_sales_df.withColumn(
    "price_category",
    when(col("price") > 1000, "High")
    .when(col("price").between(500, 1000), "Medium")
    .otherwise("Low")
)

#  Compute running total & ranking per customer**
window_spec = Window.partitionBy("customer_id").orderBy(col("transaction_date"))

running_total_df = categorized_df.withColumn("running_total", sum(col("price") * col("quantity")).over(window_spec)) \
                                 .withColumn("rank", dense_rank().over(window_spec))

# Aggregate total sales per product category**
category_sales_df = running_total_df.groupBy("category").agg(
    sum(col("price") * col("quantity")).alias("total_sales"),
    avg(col("price")).alias("average_price"),
    count("*").alias("transaction_count")
)

# Pivot sales data by category**
pivot_df = running_total_df.groupBy("customer_id").pivot("category").sum("price")



# Show results
print("Filtered & Categorized Data:")
running_total_df.show()

print("Aggregated Sales Data:")
category_sales_df.show()

print("Pivoted Sales Data:")
pivot_df.show()
