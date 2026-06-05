from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test") \
    .getOrCreate()

data = [("Alice", 25), ("Bob", 30)]

df = spark.createDataFrame(data, ["name", "age"])

df.show()

spark.stop()