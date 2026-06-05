from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BRONZE_PATH = "/data/bronze"
SILVER_PATH = "/data/silver"

VALID_EVENTS = [
    "page_view",
    "product_view",
    "add_to_cart",
    "checkout",
    "purchase"
]


def create_spark_session():
    return SparkSession.builder \
        .appName("SilverLayerJob") \
        .getOrCreate()


def main():
    spark = create_spark_session()

    df = spark.read.parquet(BRONZE_PATH)

    print("=== Bronze Count ===")
    print(df.count())

    # 1. 중복 제거
    df = df.dropDuplicates(["event_id"])

    # 2. 필수 null 제거
    df = df.dropna(subset=[
        "event_id",
        "user_id",
        "session_id",
        "event_type",
        "event_timestamp"
    ])

    # 3. 유효 이벤트만 유지
    df = df.filter(
        col("event_type").isin(VALID_EVENTS)
    )

    # 4. purchase validation
    df = df.filter(
        ~(
            (col("event_type") == "purchase") &
            col("price").isNull()
        )
    )

    print("=== Cleaned Count ===")
    print(df.count())

    print("=== Cleaned Sample ===")
    df.show(10, truncate=False)

    df.write \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .parquet(SILVER_PATH)

    print(f"Silver layer saved to {SILVER_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()