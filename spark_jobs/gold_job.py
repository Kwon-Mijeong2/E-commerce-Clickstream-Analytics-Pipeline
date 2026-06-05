from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    count,
    sum as spark_sum
)

SILVER_PATH = "/data/silver"
GOLD_PATH = "/data/gold"


def create_spark_session():
    return SparkSession.builder \
        .appName("GoldLayerJob") \
        .getOrCreate()


def save(df, path):
    df.write.mode("overwrite").parquet(path)


def conversion_funnel(df):
    return df.groupBy("event_type") \
        .agg(countDistinct("user_id").alias("users")) \
        .orderBy("users", ascending=False)


def top_products(df):
    return df.filter(col("event_type") == "purchase") \
        .groupBy("product_id") \
        .agg(count("*").alias("purchase_count")) \
        .orderBy(col("purchase_count").desc())


def device_conversion(df):
    return df.filter(col("event_type") == "purchase") \
        .groupBy("device") \
        .agg(countDistinct("user_id").alias("purchasers")) \
        .orderBy(col("purchasers").desc())


def daily_revenue(df):
    return df.filter(col("event_type") == "purchase") \
        .groupBy("event_date") \
        .agg(
            spark_sum("price").alias("revenue"),
            count("*").alias("purchase_count")
        ) \
        .orderBy("event_date")


def main():
    spark = create_spark_session()

    df = spark.read.parquet(SILVER_PATH)

    funnel_df = conversion_funnel(df)
    products_df = top_products(df)
    device_df = device_conversion(df)
    revenue_df = daily_revenue(df)

    print("=== Conversion Funnel ===")
    funnel_df.show()

    print("=== Top Products ===")
    products_df.show(10)

    print("=== Device Conversion ===")
    device_df.show()

    print("=== Daily Revenue ===")
    revenue_df.show()

    save(funnel_df, f"{GOLD_PATH}/funnel")
    save(products_df, f"{GOLD_PATH}/top_products")
    save(device_df, f"{GOLD_PATH}/device_conversion")
    save(revenue_df, f"{GOLD_PATH}/daily_revenue")

    print(f"Gold layer saved to {GOLD_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()