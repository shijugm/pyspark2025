from pyspark.sql.window import Window
from pyspark.sql.functions import  avg
from src.utils.helpers import SparkSessionManager


def run():
    spark = SparkSessionManager.get_spark_session()

    # Create DataFrame with sales data
    data = [
    (1, "2023-04-01", 100),
    (1, "2023-04-02", 150),
    (1, "2023-04-03", 200),
    (2, "2023-04-01", 50),
    (2, "2023-04-02", 75),
    (2, "2023-04-03", 125)
    ]

    df = spark.createDataFrame(data, ["customer_id", "date", "sales"])

    # Calculate moving average sales per customer for the last 2 days
    window = Window.orderBy("date").rowsBetween(-1, 0)
    df_with_moving_average = df.withColumn( "moving_average", avg("sales").over(window))

    df_with_moving_average.show()
    # df.show()

if __name__ == "__main__":
    print ("Start")
    run()