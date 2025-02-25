from pyspark.sql.types import FloatType , StringType
from pyspark.sql.functions import udf
from src.utils.helpers import SparkSessionManager
from src.utils.logger import get_logger



def to_uppercase(str):
    # Return the uppercase string
    # Ternary  : value_if_true if condition else value_if_false
    return str.upper() if str else None




def run():
    
    spark = SparkSessionManager.get_spark_session()
    
    # Register UDF with PySpark
    uppercase_udf = udf(to_uppercase, StringType())
    
    

    # Dataframe data - List of tuples . text strings
    str_df_data = [("a" ,) , ("bbb",) , ("cCa",)]
    
    # Dataframe columns 
    str_df_cols = ["txt"]
    str_df = spark.createDataFrame(str_df_data, str_df_cols)
    
    logger.info("DataFrame created")

    str_df.show()

    # Add column with the uppercase transformation
    str_df_ucase = str_df.withColumn("ucase" , uppercase_udf(str_df.txt))

    logger.info("String transformation with UDF completed")

    str_df_ucase.show()

if __name__ == "__main__":
    # initialize the logger
    logger = get_logger(__name__)
    logger.info("Starting spark example UDF")

    run()