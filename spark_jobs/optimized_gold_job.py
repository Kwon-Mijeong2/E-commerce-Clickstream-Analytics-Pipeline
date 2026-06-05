from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    count,
    sum as spark_sum
)

SILVER_PATH = "/data/silver"
GOLD_PATH = "/data/gold_optimized"


def create_spark():
    return SparkSession.builder \
        .appName("OptimizedGoldLayer") \
        .getOrCreate()


def save(df, path):
    df.write.mode("overwrite").parquet(path)


def main():
    spark = create_spark()

    df = spark.read.parquet(SILVER_PATH)

    print("=== Initial Partition Count ===")
    print(df.rdd.getNumPartitions())

    # 1. Repartition
    df = df.repartition(8)

    print("=== After Repartition ===")
    print(df.rdd.getNumPartitions())

    # 2. Cache
    df.cache()

    # Materialize cache
    df.count()

    purchase_df = df.filter(col("event_type") == "purchase").cache()
    purchase_df.count()

    # Funnel
    funnel_df = df.groupBy("event_type") \
        .agg(countDistinct("user_id").alias("users"))

    # Top Products
    top_products_df = purchase_df.groupBy("product_id") \
        .agg(count("*").alias("purchase_count")) \
        .orderBy(col("purchase_count").desc())

    # Device Conversion
    device_df = purchase_df.groupBy("device") \
        .agg(countDistinct("user_id").alias("purchasers"))

    # Daily Revenue
    revenue_df = purchase_df.groupBy("event_date") \
        .agg(
            spark_sum("price").alias("revenue"),
            count("*").alias("purchase_count")
        )

    print("=== Execution Plan ===")
    revenue_df.explain(True)

    save(funnel_df, f"{GOLD_PATH}/funnel")
    save(top_products_df, f"{GOLD_PATH}/top_products")
    save(device_df, f"{GOLD_PATH}/device_conversion")
    save(revenue_df, f"{GOLD_PATH}/daily_revenue")

    print("Optimized Gold layer completed.")

    spark.stop()


if __name__ == "__main__":
    main()