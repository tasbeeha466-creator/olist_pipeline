import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pytest
from config.local_config import GOLD_DIR

DELIVERY_COMPLAINT_REFERENCE = 67.0
LATE_DELIVERY_BAD_REVIEW_MULTIPLIER = 4.0
TOLERANCE = 20.0


def load_gold(table_name):
    path = os.path.join(GOLD_DIR, table_name)
    if not os.path.exists(path):
        return None
    return pd.read_parquet(path)


def test_gold_delivery_kpis_exists():
    df = load_gold("delivery_kpis")
    assert df is not None, "Gold delivery_kpis table does not exist"


def test_gold_review_kpis_exists():
    df = load_gold("review_kpis")
    assert df is not None, "Gold review_kpis table does not exist"


def test_gold_payment_kpis_exists():
    df = load_gold("payment_kpis")
    assert df is not None, "Gold payment_kpis table does not exist"


def test_gold_category_kpis_exists():
    df = load_gold("category_kpis")
    assert df is not None, "Gold category_kpis table does not exist"


def test_delivery_complaint_pct_near_reference():
    df = load_gold("review_kpis")
    actual = df["delivery_complaint_in_bad_pct"].mean()
    lower = DELIVERY_COMPLAINT_REFERENCE - TOLERANCE
    upper = DELIVERY_COMPLAINT_REFERENCE + TOLERANCE
    assert lower <= actual <= upper, (
        f"Delivery complaint % = {actual:.1f}% — expected near {DELIVERY_COMPLAINT_REFERENCE}% "
        f"(tolerance ±{TOLERANCE}%). NLP logic may need review."
    )


def test_late_delivery_bad_review_rate():
    df = load_gold("delivery_kpis")
    assert df is not None, "delivery_kpis missing"
    assert "late_pct" in df.columns, "late_pct column missing"
    avg_late_pct = df["late_pct"].mean()
    assert avg_late_pct >= 0, "late_pct cannot be negative"


def test_avg_review_score_reasonable():
    df = load_gold("review_kpis")
    avg_score = df["avg_score"].mean()
    assert 1.0 <= avg_score <= 5.0, f"Avg review score {avg_score} outside valid range"


def test_payment_types_present():
    df = load_gold("payment_kpis")
    assert "credit_card" in df["payment_type"].values, "credit_card payment type missing"


def test_installment_pct_near_reference():
    df = load_gold("payment_kpis")
    avg_installment = df["installment_pct"].mean()
    assert 20.0 <= avg_installment <= 80.0, (
        f"Installment % = {avg_installment:.1f}% — expected near 50%"
    )


def test_top_category_exists():
    df = load_gold("category_kpis")
    df = df.dropna(subset=["product_category_name_english"])
    assert len(df) > 0, "No categories in gold layer"
    top = df.nlargest(1, "total_orders").iloc[0]
    assert top["total_orders"] > 0, "Top category has 0 orders"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])