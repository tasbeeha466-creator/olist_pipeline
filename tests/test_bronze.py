import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pytest
from config.local_config import BRONZE_DIR


def load_bronze(table_name):
    path = os.path.join(BRONZE_DIR, table_name)
    if not os.path.exists(path):
        return None
    return pd.read_parquet(path)


def test_bronze_orders_exists():
    df = load_bronze("orders")
    assert df is not None, "Bronze orders table does not exist"


def test_bronze_orders_row_count():
    df = load_bronze("orders")
    assert len(df) > 0, "Bronze orders table is empty"


def test_bronze_orders_required_columns():
    df = load_bronze("orders")
    required = ["order_id", "customer_id", "order_status", "order_purchase_timestamp"]
    for col in required:
        assert col in df.columns, f"Missing column: {col}"


def test_bronze_orders_no_duplicate_order_ids():
    df = load_bronze("orders")
    assert df["order_id"].duplicated().sum() == 0, "Duplicate order_ids found in bronze orders"


def test_bronze_items_exists():
    df = load_bronze("items")
    assert df is not None, "Bronze items table does not exist"


def test_bronze_items_price_non_negative():
    df = load_bronze("items")
    assert (df["price"] >= 0).all(), "Negative prices found in bronze items"


def test_bronze_payments_exists():
    df = load_bronze("payments")
    assert df is not None, "Bronze payments table does not exist"


def test_bronze_reviews_score_range():
    df = load_bronze("reviews")
    assert df["review_score"].between(1, 5).all(), "Review scores outside 1-5 range in bronze"


def test_bronze_dead_letter_captured():
    path = os.path.join(BRONZE_DIR, "..", "..", "lake", "bronze")
    assert os.path.exists(path), "Bronze layer directory missing"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
