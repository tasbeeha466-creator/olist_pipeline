import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ["HADOOP_HOME"] = r"C:\winutils"
os.environ["PATH"] = r"C:\winutils\bin" + ";" + os.environ["PATH"]
os.environ["JAVA_HOME"] = r"C:\Program Files\Microsoft\jdk-11.0.22.7-hotspot"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from config.local_config import (
    BRONZE_DIR, CHECKPOINT_DIR, SPARK_APP_NAME, SPARK_MASTER,
    SPARK_DRIVER_MEMORY, SPARK_SHUFFLE_PARTITIONS, SPARK_KAFKA_PACKAGE
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

TOPICS = {
    "orders": "olist_orders",
    "items": "olist_items",
    "customers": "olist_customers",
    "payments": "olist_payments",
    "reviews": "olist_reviews",
    "products": "olist_products",
    "sellers": "olist_sellers",
}

SCHEMAS = {
    "orders": StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_status", StringType()),
        StructField("order_purchase_timestamp", StringType()),
        StructField("order_approved_at", StringType()),
        StructField("order_delivered_carrier_date", StringType()),
        StructField("order_delivered_customer_date", StringType()),
        StructField("order_estimated_delivery_date", StringType()),
    ]),
    "items": StructType([
        StructField("order_id", StringType()),
        StructField("order_item_id", IntegerType()),
        StructField("product_id", StringType()),
        StructField("seller_id", StringType()),
        StructField("shipping_limit_date", StringType()),
        StructField("price", DoubleType()),
        StructField("freight_value", DoubleType()),
    ]),
    "customers": StructType([
        StructField("customer_id", StringType()),
        StructField("customer_unique_id", StringType()),
        StructField("customer_zip_code_prefix", StringType()),
        StructField("customer_city", StringType()),
        StructField("customer_state", StringType()),
    ]),
    "payments": StructType([
        StructField("order_id", StringType()),
        StructField("payment_sequential", IntegerType()),
        StructField("payment_type", StringType()),
        StructField("payment_installments", IntegerType()),
        StructField("payment_value", DoubleType()),
    ]),
    "reviews": StructType([
        StructField("review_id", StringType()),
        StructField("order_id", StringType()),
        StructField("review_score", IntegerType()),
        StructField("review_comment_title", StringType()),
        StructField("review_comment_message", StringType()),
        StructField("review_creation_date", StringType()),
        StructField("review_answer_timestamp", StringType()),
    ]),
    "products": StructType([
        StructField("product_id", StringType()),
        StructField("product_category_name", StringType()),
        StructField("product_weight_g", DoubleType()),
        StructField("product_length_cm", DoubleType()),
        StructField("product_height_cm", DoubleType()),
        StructField("product_width_cm", DoubleType()),
    ]),
    "sellers": StructType([
        StructField("seller_id", StringType()),
        StructField("seller_zip_code_prefix", StringType()),
        StructField("seller_city", StringType()),
        StructField("seller_state", StringType()),
    ]),
}

def create_spark():
    return (
        SparkSession.builder
        .appName(f"{SPARK_APP_NAME}_Bronze")
        .master(SPARK_MASTER)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.parquet.filterPushdown", "true")
        .getOrCreate()
    )


def write_bronze(df, table_name):
    checkpoint = os.path.join(CHECKPOINT_DIR, f"bronze_{table_name}")
    output = os.path.join(BRONZE_DIR, table_name)
    return (
        df.writeStream
        .format("parquet")
        .option("checkpointLocation", checkpoint)
        .option("path", output)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("maxFilesPerTrigger", "1")
        .start()
    )

def read_topic(spark, topic_name, schema):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_topic", F.lit(topic_name))
    )

def write_bronze(df, table_name):
    checkpoint = os.path.join(CHECKPOINT_DIR, f"bronze_{table_name}")
    output = os.path.join(BRONZE_DIR, table_name)
    return (
        df.writeStream
        .format("parquet")
        .option("checkpointLocation", checkpoint)
        .option("path", output)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

def run():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    queries = []
    for table_name, topic_key in [
        ("orders", "orders"),
        ("items", "items"),
        ("customers", "customers"),
        ("payments", "payments"),
        ("reviews", "reviews"),
        ("products", "products"),
        ("sellers", "sellers"),
    ]:
        df = read_topic(spark, TOPICS[topic_key], SCHEMAS[table_name])
        q = write_bronze(df, table_name)
        queries.append(q)
        print(f"Bronze stream started: {table_name}")
    
    for q in queries:
        q.awaitTermination()

if __name__ == "__main__":
    run()