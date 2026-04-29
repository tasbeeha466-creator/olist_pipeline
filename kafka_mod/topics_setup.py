from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.local_config import KAFKA_BOOTSTRAP_SERVERS, TOPICS
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

def create_topics():
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    topic_list = [
        NewTopic(name=name, num_partitions=3, replication_factor=1)
        for name in TOPICS.values()
    ]
    try:
        admin.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Created {len(topic_list)} Kafka topics.")
    except TopicAlreadyExistsError:
        print("Topics already exist, skipping.")
    finally:
        admin.close()

if __name__ == "__main__":
    create_topics()
    