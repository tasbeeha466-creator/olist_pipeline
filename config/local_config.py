import os

ENV = "local"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
CHECKPOINT_DIR = os.path.join(BASE_DIR, "checkpoints")
LAKE_DIR = os.path.join(BASE_DIR, "lake")
LOGS_DIR = os.path.join(BASE_DIR, "logs")

BRONZE_DIR = os.path.join(LAKE_DIR, "bronze")
SILVER_DIR = os.path.join(LAKE_DIR, "silver")
GOLD_DIR = os.path.join(LAKE_DIR, "gold")

HADOOP_HOME = r"C:\winutils"
WINUTILS_PATH = r"C:\winutils\bin"
JAVA_HOME = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.0.0-hotspot"

SPARK_APP_NAME = "OlistStreamingPipeline"
SPARK_MASTER = "local[*]"
SPARK_DRIVER_MEMORY = "2g"
SPARK_SHUFFLE_PARTITIONS = "4"
SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

SAMPLE_SIZE = 99441
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"

for d in [DATA_DIR, CHECKPOINT_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, LOGS_DIR]:
    os.makedirs(d, exist_ok=True)
    