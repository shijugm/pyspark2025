from pyspark.sql import SparkSession

class SparkSessionManager:
    """Spark session"""
    
    @staticmethod
    def get_spark_session():
        
        # TODO: Exception handling
        """Initialize a spark session"""
        spark = SparkSession \
            .builder \
            .appName("pyspark2025") \
            .getOrCreate()
        
        return spark 