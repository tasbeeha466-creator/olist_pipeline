import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pytest
from config.local_config import SILVER_DIR


def load_silver(table_name):
    path = os.path.join(SILVER_DIR, table_name)
    if not os.path.exists(path):
        return None
    return pd.read_parquet(path)


def test_silver_orders_exists():
    df = load_silver("orders")
    assert df is not None, "Silver orders table does not exist"


def test_silver_orders_only_delivered():
    df = load_silver("orders")
    assert (df["order_status"] == "delivered").all(), "Non-delivered orders found in silver"


def test_silver_orders_no_null_delivery_date():
    df = load_silver("orders")
    assert df["order_delivered_customer_date"].notna().all(), "Null delivery dates in silver orders"


def test_silver_orders_delivery_days_non_negative():
    df = load_silver("orders")
    assert (df["delivery_days"] >= 0).all(), "Negative delivery days in silver orders"


def test_silver_orders_has_delay_columns():
    df = load_silver("orders")
    for col in ["delivery_days", "estimated_days", "delay_days", "is_late"]:
        assert col in df.columns, f"Missing feature column: {col}"


def test_silver_reviews_has_nlp_columns():
    df = load_silver("reviews")
    for col in ["sentiment", "is_delivery_complaint", "is_bad_review", "delivery_complaint_in_bad_review"]:
        assert col in df.columns, f"Missing NLP column: {col}"


def test_silver_reviews_sentiment_valid_values():
    df = load_silver("reviews")
    valid = {"positive", "negative", "neutral"}
    assert set(df["sentiment"].unique()).issubset(valid), "Invalid sentiment values in silver reviews"


def test_silver_payments_no_negative_values():
    df = load_silver("payments")
    assert (df["payment_value"] >= 0).all(), "Negative payment values in silver payments"


def test_silver_items_has_total_value():
    df = load_silver("items")
    assert "item_total_value" in df.columns, "Missing item_total_value in silver items"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])