KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

TOPICS = {
    "orders": "olist.orders",
    "items": "olist.items",
    "customers": "olist.customers",
    "payments": "olist.payments",
    "reviews": "olist.reviews",
    "products": "olist.products",
    "sellers": "olist.sellers",
    "dead_letter": "olist.dead_letter"
}

KAFKA_BATCH_DELAY = 0.05
KAFKA_ACKS = "all"
KAFKA_RETRIES = 3
KAFKA_LINGER_MS = 10
KAFKA_NUM_PARTITIONS = 3
KAFKA_REPLICATION_FACTOR = 1
