import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ["HADOOP_HOME"] = r"C:\winutils"
os.environ["PATH"] = r"C:\winutils\bin" + ";" + os.environ["PATH"]
os.environ["JAVA_HOME"] = r"C:\Program Files\Microsoft\jdk-11.0.22.7-hotspot"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config.local_config import (
    SILVER_DIR, GOLD_DIR, CHECKPOINT_DIR,
    SPARK_APP_NAME, SPARK_MASTER, SPARK_DRIVER_MEMORY, SPARK_SHUFFLE_PARTITIONS
)


def create_spark():
    return (
        SparkSession.builder
        .appName(f"{SPARK_APP_NAME}_Gold")
        .master(SPARK_MASTER)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config ("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )


def write(spark_context, df, name):
    checkpoint = os.path.join(CHECKPOINT_DIR, f"gold_{name}")
    output = os.path.join(GOLD_DIR, name)
    return (
        df.writeStream
        .format("parquet")
        .option("checkpointLocation", checkpoint)
        .option("path", output)
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )


def run():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    orders_path = os.path.join(SILVER_DIR, "orders")
    orders_schema = spark.read.parquet(orders_path).schema
    orders = spark.readStream.format("parquet").schema(orders_schema).load(orders_path)

    reviews_path = os.path.join(SILVER_DIR, "reviews")
    reviews_schema = spark.read.parquet(reviews_path).schema
    reviews = spark.readStream.format("parquet").schema(reviews_schema).load(reviews_path)

    payments_path = os.path.join(SILVER_DIR, "payments")
    payments_schema = spark.read.parquet(payments_path).schema
    payments = spark.readStream.format("parquet").schema(payments_schema).load(payments_path)

    items_path = os.path.join(SILVER_DIR, "items")
    items_schema = spark.read.parquet(items_path).schema
    items = spark.readStream.format("parquet").schema(items_schema).load(items_path)

    delivery = orders.select(
        "order_id", "customer_state", "delivery_days",
        "delay_days", "is_late", "estimated_days",
        "order_purchase_timestamp"
    )

    review = reviews.select(
        "order_id", "review_score", "is_bad_review",
        "is_delivery_complaint", "delivery_complaint_in_bad_review", "sentiment"
    )

    payment = payments.select(
        "order_id", "payment_type", "payment_value",
        "payment_installments"
    ).withColumn("used_installments", F.col("payment_installments") > 1)

    category = items.select(
        "order_id", "product_category_name_english",
        "price", "freight_value", "item_total_value"
    )

    queries = [
        write(spark, delivery, "delivery_kpis"),
        write(spark, review, "review_kpis"),
        write(spark, payment, "payment_kpis"),
        write(spark, category, "category_kpis"),
    ]

    print("Gold streams started.")
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    run()