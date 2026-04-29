import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ["HADOOP_HOME"] = r"C:\winutils"
os.environ["PATH"] = r"C:\winutils\bin" + ";" + os.environ["PATH"]
os.environ["JAVA_HOME"] = r"C:\Program Files\Microsoft\jdk-11.0.22.7-hotspot"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, BooleanType
from config.local_config import (
    BRONZE_DIR, SILVER_DIR, CHECKPOINT_DIR, DATA_DIR,
    SPARK_APP_NAME, SPARK_MASTER, SPARK_DRIVER_MEMORY, SPARK_SHUFFLE_PARTITIONS
)
from nlp.processor import score_sentiment, is_delivery_complaint

sentiment_udf = F.udf(
    lambda msg, title: score_sentiment(f"{title or ''} {msg or ''}"),
    StringType()
)

delivery_udf = F.udf(
    lambda msg, title: is_delivery_complaint(f"{title or ''} {msg or ''}"),
    BooleanType()
)

def create_spark():
    return (
        SparkSession.builder
        .appName(f"{SPARK_APP_NAME}_Silver")
        .master(SPARK_MASTER)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.python.worker.reuse", "true")
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.python.worker.timeout", "600")
        .getOrCreate()
    )

def load_static(spark):
    translation = spark.read.csv(
        os.path.join(DATA_DIR, "sample_translation.csv"),
        header=True, inferSchema=True
    )
    customers = spark.read.parquet(os.path.join(BRONZE_DIR, "customers"))
    products = (
        spark.read.parquet(os.path.join(BRONZE_DIR, "products"))
        .join(translation, on="product_category_name", how="left")
    )
    sellers = spark.read.parquet(os.path.join(BRONZE_DIR, "sellers"))
    return customers, products, sellers

def process_orders(spark, customers):
    date_cols = [
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    bronze_path = os.path.join(BRONZE_DIR, "orders")
    orders = spark.read.parquet(os.path.join(BRONZE_DIR, "orders"))
    stream = spark.readStream.format("parquet").schema(orders.schema).load(os.path.join(BRONZE_DIR, "orders"))
    
    for col in date_cols:
        stream = stream.withColumn(col, F.to_timestamp(col))
    
    stream = (
        stream
        .filter(F.col("order_status") == "delivered")
        .filter(F.col("order_delivered_customer_date").isNotNull())
        .filter(F.col("order_purchase_timestamp").isNotNull())
        .withColumn("delivery_days", F.round(
            (F.unix_timestamp("order_delivered_customer_date") -
             F.unix_timestamp("order_purchase_timestamp")) / 86400, 2
        ))
        .withColumn("estimated_days", F.round(
            (F.unix_timestamp("order_estimated_delivery_date") -
             F.unix_timestamp("order_purchase_timestamp")) / 86400, 2
        ))
        .withColumn("delay_days", F.round(
            F.col("delivery_days") - F.col("estimated_days"), 2
        ))
        .withColumn("is_late", F.col("delay_days") > 0)
        .withColumn("delivery_days",
            F.when(F.col("delivery_days") < 0, 0).otherwise(F.col("delivery_days"))
        )
        .withColumn("approval_hours", F.round(
            (F.unix_timestamp("order_approved_at") -
             F.unix_timestamp("order_purchase_timestamp")) / 3600, 2
        ))
        .join(
            customers.select("customer_id", "customer_unique_id", "customer_state", "customer_city"),
            on="customer_id", how="left"
        )
    )
    return stream

def process_items(spark, products, sellers):
    bronze_path = os.path.join(BRONZE_DIR, "items")
    items = spark.read.parquet(os.path.join(BRONZE_DIR, "items"))
    stream = spark.readStream.format("parquet").schema(items.schema).load(os.path.join(BRONZE_DIR, "items"))
    stream = (
        stream
        .withColumn("item_total_value", F.col("price") + F.col("freight_value"))
        .join(
            products.select("product_id", "product_category_name", "product_category_name_english"),
            on="product_id", how="left"
        )
        .join(
            sellers.select("seller_id", "seller_state", "seller_city"),
            on="seller_id", how="left"
        )
    )
    return stream
def process_payments(spark):
    bronze_path = os.path.join(BRONZE_DIR, "payments")
    payments = spark.read.parquet(os.path.join(BRONZE_DIR, "payments"))
    stream = spark.readStream.format("parquet").schema(payments.schema).load(os.path.join(BRONZE_DIR, "payments"))
    stream = (
        stream
        .withColumn("used_installments", F.col("payment_installments") > 1)
        .withColumn("payment_value",
            F.when(F.col("payment_value") < 0, 0).otherwise(F.col("payment_value"))
        )
    )
    return stream
def process_reviews(spark):
    bronze_path = os.path.join(BRONZE_DIR, "reviews")
    schema = spark.read.parquet(bronze_path).schema
    stream = spark.readStream.format("parquet").schema(schema).load(bronze_path)
    
    stream = (
        stream
        .withColumn("review_creation_date", F.to_timestamp("review_creation_date"))
        .withColumn("is_bad_review", F.col("review_score") <= 3)
        .withColumn("is_delivery_complaint",
            F.when(
                F.lower(F.coalesce(F.col("review_comment_message"), F.lit(""))).rlike(
                    "atraso|atrasou|nao recebi|nao chegou|demora|demorou|prazo|entrega|extraviado|perdido"
                ), True
            ).otherwise(False)
        )
        .withColumn("sentiment",
            F.when(
                F.lower(F.coalesce(F.col("review_comment_message"), F.lit(""))).rlike(
                    "pessimo|horrivel|problema|defeito|quebrado|ruim|cancelado"
                ), F.lit("negative")
            ).when(
                F.lower(F.coalesce(F.col("review_comment_message"), F.lit(""))).rlike(
                    "excelente|otimo|perfeito|rapido|recomendo|amei|qualidade"
                ), F.lit("positive")
            ).otherwise(F.lit("neutral"))
        )
        .withColumn("delivery_complaint_in_bad_review",
            F.col("is_bad_review") & F.col("is_delivery_complaint")
        )
        .withWatermark("review_creation_date", "30 days")
    )
    return stream

def write_silver(df, table_name):
    checkpoint = os.path.join(CHECKPOINT_DIR, f"silver_{table_name}")
    output = os.path.join(SILVER_DIR, table_name)
    return (
        df.writeStream
        .format("parquet")
        .option("checkpointLocation", checkpoint)
        .option("path", output)
        .outputMode("append")
        .trigger(processingTime="15 seconds")
        .start()
    )

def run():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    customers, products, sellers = load_static(spark)
    
    queries = [
        write_silver(process_orders(spark, customers), "orders"),
        write_silver(process_items(spark, products, sellers), "items"),
        write_silver(process_payments(spark), "payments"),
        write_silver(process_reviews(spark), "reviews"),
    ]
    
    print("Silver streams started.")
    for q in queries:
        q.awaitTermination()

if __name__ == "__main__":
    run()
