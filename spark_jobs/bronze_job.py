from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
from pyspark.sql.functions import to_timestamp, to_date, col

RAW_PATH = "/data/raw"
BRONZE_PATH = "/data/bronze"


def create_spark_session():
    return SparkSession.builder \
        .appName("BronzeLayerJob") \
        .getOrCreate()


def define_schema():
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("device", StringType(), True),
        StructField("country", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])


def main():
    spark = create_spark_session()

    schema = define_schema()

    df = spark.read \
        .schema(schema) \
        .json(RAW_PATH)

    print("=== Raw Schema ===")
    df.printSchema()

    df = df.withColumn(
        "event_timestamp",
        to_timestamp(col("timestamp"))
    )

    df = df.withColumn(
        "event_date",
        to_date(col("event_timestamp"))
    )

    print("=== Sample Data ===")
    df.show(10, truncate=False)

    df.write \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .parquet(BRONZE_PATH)

    print(f"Bronze layer saved to {BRONZE_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()