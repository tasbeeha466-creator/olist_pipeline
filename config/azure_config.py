import os

ENV = "azure"

AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT", "")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY", "")
AZURE_CONTAINER = os.getenv("AZURE_CONTAINER", "olist-lake")

BRONZE_DIR = f"abfss://{AZURE_CONTAINER}@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/bronze"
SILVER_DIR = f"abfss://{AZURE_CONTAINER}@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/silver"
GOLD_DIR = f"abfss://{AZURE_CONTAINER}@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/gold"
CHECKPOINT_DIR = f"abfss://{AZURE_CONTAINER}@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints"

EVENT_HUB_NAMESPACE = os.getenv("EVENT_HUB_NAMESPACE", "")
EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING", "")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "olist-orders")
KAFKA_BOOTSTRAP_SERVERS = f"{EVENT_HUB_NAMESPACE}.servicebus.windows.net:9093" if EVENT_HUB_NAMESPACE else ""

SPARK_APP_NAME = "OlistStreamingPipeline"
SPARK_MASTER = "yarn"
SPARK_DRIVER_MEMORY = "8g"
SPARK_SHUFFLE_PARTITIONS = "16"

SAMPLE_SIZE = None
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
if IS_DATABRICKS:
    DATA_DIR = "/dbfs/data"
    LOGS_DIR = "/dbfs/logs"
else:
    import tempfile
    DATA_DIR = tempfile.gettempdir() + "/olist_data"
    LOGS_DIR = tempfile.gettempdir() + "/olist_logs"

if not AZURE_STORAGE_ACCOUNT or not AZURE_STORAGE_KEY:
    print("WARNING: Azure credentials not set")

print("Azure config loaded")
print(f"STORAGE_ACCOUNT: {AZURE_STORAGE_ACCOUNT if AZURE_STORAGE_ACCOUNT else 'NOT SET'}")
print(f"CONTAINER: {AZURE_CONTAINER}")
print(f"EVENT_HUB: {EVENT_HUB_NAME}")
