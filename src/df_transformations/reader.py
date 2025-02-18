from pyspark.sql.types import *
from src.utils.helpers import SparkSessionManager


def run():
    spark = SparkSessionManager.get_spark_session()


    # Reading a CSV file with options 

    df_src = spark.read. \
        options(header=True,
               inferSchema=True,
               delimiter=","
               ). \
        csv("data/tst.csv")
    df_src.printSchema()

    # or 

    # Define read options
    read_csv_opts = {
        "header": "True",
        "inferSchema": "True",
        "delimiter": ","
    }

    df_src = spark.read.options(**read_csv_opts). \
        csv("data/tst.csv")
    
    df_src.printSchema()   

    
    # Reading a CSV file and passing the schema 

    src_csv_schema = StructType([
        StructField("EmployeeID",IntegerType(),True),
        StructField("Employee Name",StringType(),True)
    ]) 

    df_src = spark.read \
        .option("header",True) \
        .schema(src_csv_schema) \
        .csv("data/tst.csv")
    
    df_src.printSchema()   


if __name__ == "__main__":
    print ("Start")
    run()