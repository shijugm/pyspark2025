"""
Spark streaming read example 
Run local docker
Run rebalance_producer example 

"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import  col
# from pyspark.sql.functions import date_format , from_json
# from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql.avro.functions import from_avro
# from src.utils.helpers import SparkSessionManager


# TODO: Fix version dependency.
#  Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.repositories", "https://packages.confluent.io/maven/") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.2.1,"
            "org.apache.spark:spark-avro_2.12:3.2.1,"
            "io.confluent:kafka-avro-serializer:7.5.0") \
    .getOrCreate()


avro_schema = """
{
    "namespace": "shijum.kafkapython.example.avro",
    "name": "UserDetails",
    "type": "record",
    "fields": [
        {
            "name": "name",
            "type": "string"
        },
        {
            "name": "favorite_number",
            "type": "long"
        },
        {
            "name": "favorite_color",
            "type": "string"
        }
    ]
}
"""

# ✅ Read Data from Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "rebalance_hello_world") \
    .option("startingOffsets", "earliest") \
    .load()


# Deserialize Avro Messages
df_parsed = df_stream.select(
    from_avro(col("value"), avro_schema).alias("data")
).select("data.*")

# JSON example
# schema = StructType() \
#     .add("name", StringType()) \
#     .add("favourite_number", IntegerType()) \
#     .add("favourite_colour", StringType())

# df_parsed = df_stream.selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), schema).alias("data")) \
#     .select("data.*")


# Print to the console
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


# Writing to a parquet
# query =  df_parsed.writeStream \
#     .format("parquet") \
#     .option("path", "/output/parquet_output/") \
#     .option("checkpointLocation", "/output/parquet_checkpoint/") \
#     .partitionBy("favourite_number").outputMode("append") \
#     .start()


query.awaitTermination()
