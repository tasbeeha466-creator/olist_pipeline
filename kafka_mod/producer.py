import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.local_config import DATA_DIR
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, TOPICS, KAFKA_BATCH_DELAY
import json
import time
import pandas as pd
from kafka import KafkaProducer
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.local_config import DATA_DIR
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

TOPICS = {
    "orders": "olist_orders",
    "items": "olist_items",
    "customers": "olist_customers",
    "payments": "olist_payments",
    "reviews": "olist_reviews",
    "products": "olist_products",
    "sellers": "olist_sellers",
    "dead_letter": "olist_dead_letter",
}

KAFKA_BATCH_DELAY = 0.01

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
        retries=5,
        enable_idempotence=True,
        max_in_flight_requests_per_connection=1,
        linger_ms=10
    )

def validate_row(row, required_fields):
    for field in required_fields:
        if field not in row or pd.isna(row.get(field)):
            return False
    return True

def produce_table(producer, filepath, topic, required_fields):
    dead_letter_topic = TOPICS["dead_letter"]
    df = pd.read_csv(filepath)
    sent = 0
    dead = 0
    
    for _, row in df.iterrows():
        record = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        if validate_row(record, required_fields):
            producer.send(topic, value=record)
            sent += 1
        else:
            record["_dead_reason"] = "missing_required_fields"
            record["_source_topic"] = topic
            producer.send(dead_letter_topic, value=record)
            dead += 1
        time.sleep(KAFKA_BATCH_DELAY)
    
    producer.flush()
    print(f"{topic}: {sent} sent | {dead} dead letters")
    return sent, dead

def run_producer():
    producer = create_producer()
    
    tables = [
        (
            os.path.join(DATA_DIR, "sample_orders.csv"),
            TOPICS["orders"],
            ["order_id", "customer_id", "order_status"]
        ),
        (
            os.path.join(DATA_DIR, "sample_items.csv"),
            TOPICS["items"],
            ["order_id", "product_id", "seller_id", "price"]
        ),
        (
            os.path.join(DATA_DIR, "sample_customers.csv"),
            TOPICS["customers"],
            ["customer_id", "customer_unique_id"]
        ),
        (
            os.path.join(DATA_DIR, "sample_payments.csv"),
            TOPICS["payments"],
            ["order_id", "payment_type", "payment_value"]
        ),
        (
            os.path.join(DATA_DIR, "sample_reviews.csv"),
            TOPICS["reviews"],
            ["order_id", "review_score"]
        ),
        (
            os.path.join(DATA_DIR, "sample_products.csv"),
            TOPICS["products"],
            ["product_id", "product_category_name"]
        ),
        (
            os.path.join(DATA_DIR, "sample_sellers.csv"),
            TOPICS["sellers"],
            ["seller_id"]
        ),
    ]
    
    for filepath, topic, required in tables:
        produce_table(producer, filepath, topic, required)
    
    producer.close()
    print("All tables produced to Kafka.")

if __name__ == "__main__":
    run_producer()