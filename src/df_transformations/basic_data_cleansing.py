"""
Online sample for source cleansing 
Example - sales dataset containing transaction details. 
Covers
	1.Cast a column to double
    2. String manipulation
	3. Null handling
    4. Cast a date column with multiple formats
    5. Normalizing values
    6. Remove duplicates by keeping the latest value


"""


from pyspark.sql.functions import col, when, regexp_replace, trim, lit, to_date, coalesce ,row_number
from pyspark.sql.types import IntegerType, DoubleType, StringType
from pyspark.sql.window import Window
from src.utils.helpers import SparkSessionManager

 

# Transform a string column with multiple date formats into a datetime column
def date_formatter(col):
    dt_formats = ("yyyy/MM/dd", "yyyy-MM-dd" , "dd-MM-yyyy")
    # Expanding this return coalesce(*[to_date(col, f) for f in dt_formats])
    # Initialize empty list 
    coalesce_list = [] 
    # For each format in dt_formats build the list of to_date conversions 
    # This will create a [to_date(col, f1) , to_date(col,f2) , to_date(col,f3)  ]
    for f in dt_formats:
        coalesce_list.append(to_date(col , f))

    # The coalesce will return the first notnull value between the 3 formats specified. 
    # The * is needed to exapnd the list
    return coalesce(*coalesce_list)
 


def run():
    # Get spark session from helper
    spark = SparkSessionManager.get_spark_session()

    # Sample raw data
    data = [
        (1, 101, "Laptop", "Elec", "1000", -11, "2024/02/20"),
        (2, 102, "Phone", "electronics", "800.0", 2, "2024-02-21"),
        (3, 101, "Shoes", "Fashion", None, 3, "22-02-2024"),
        (4, 103, "Shirt", "Fashion", "50", 4, "2024-02-23"),
        (5, 102, "Laptop", "Electronics", "1200", 1, "2024-02-24"),
        (6, 102, "Phone", "Electronics", "800.0", 2, "2024-02-21")  
    ]

    columns = ["transaction_id", "customer_id", "product", "category", "price", "quantity", "transaction_date"]

    # Create DataFrame
    sales_df = spark.createDataFrame(data, columns)

    # Cast datatype to double 
    sales_df = sales_df.withColumn("price" , col("price").cast("double")) 
    
    # remove spaces from string
    sales_df = sales_df.withColumn("product", trim(col("product"))) \
                    .withColumn("category", trim(col("category")))

    # Normalize inconsistent category values
    category_mapping = {
        "Elec": "Electronics",
        "electronics": "Electronics",
        "Electronics": "Electronics",
        "fashion": "Fashion"
    }
    #  If the category key is present in the category mapping then use the normalised value
    for key, value in category_mapping.items():
        sales_df = sales_df.withColumn("category", when(col("category") == key, value).otherwise(col("category")))


    # Cast date column with multiple date formats 
    sales_df = sales_df.withColumn("transaction_date", date_formatter("transaction_date"))  


    #  Remove negative quantities 
    sales_df = sales_df.withColumn("quantity", when(col("quantity") < 0, lit(None)).otherwise(col("quantity")))

    




    # Drop duplicate rows .
    # Remove old rows if there is a new occurance of customer, product for a transaction_dt
    window_spec = Window.partitionBy("customer_id" , "product" , "transaction_date").orderBy(col("transaction_id").desc())
    sales_df = sales_df.withColumn("rownum", row_number().over(window_spec)) \
                       .filter(col("rownum") == 1) \
                       .drop("rownum")

    sales_df.show()

if __name__ == "__main__":
    print ("Start")
    run()