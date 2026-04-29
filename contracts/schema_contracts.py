from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class ColumnContract:
    name: str
    dtype: str
    nullable: bool = True
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    allowed_values: Optional[List[Any]] = None
    description: str = ""


@dataclass
class TableContract:
    table_name: str
    version: str
    owner: str
    description: str
    columns: List[ColumnContract]
    primary_key: List[str] = field(default_factory=list)
    business_rules: List[str] = field(default_factory=list)

    def get_required_columns(self) -> List[str]:
        return [col.name for col in self.columns if not col.nullable]

    def get_column(self, name: str) -> Optional[ColumnContract]:
        for col in self.columns:
            if col.name == name:
                return col
        return None


ORDERS_CONTRACT = TableContract(
    table_name="orders",
    version="1.0.0",
    owner="data_engineering",
    description="Core orders table — one row per order",
    primary_key=["order_id"],
    business_rules=[
        "order_delivered_customer_date must be after order_purchase_timestamp",
        "order_status must be one of: delivered, shipped, canceled, unavailable, created",
        "Only delivered orders should have delivery dates"
    ],
    columns=[
        ColumnContract("order_id", "string", nullable=False, description="Unique order identifier"),
        ColumnContract("customer_id", "string", nullable=False, description="Customer identifier per order"),
        ColumnContract("order_status", "string", nullable=False,
                      allowed_values=["delivered", "shipped", "canceled", "unavailable", "created", "approved", "processing", "invoiced"],
                      description="Current order status"),
        ColumnContract("order_purchase_timestamp", "timestamp", nullable=False, description="When order was placed"),
        ColumnContract("order_approved_at", "timestamp", nullable=True, description="Payment approval time"),
        ColumnContract("order_delivered_carrier_date", "timestamp", nullable=True, description="Handover to carrier"),
        ColumnContract("order_delivered_customer_date", "timestamp", nullable=True, description="Actual delivery date"),
        ColumnContract("order_estimated_delivery_date", "timestamp", nullable=False, description="Estimated delivery"),
    ]
)

ITEMS_CONTRACT = TableContract(
    table_name="items",
    version="1.0.0",
    owner="data_engineering",
    description="Order items — one row per item per order",
    primary_key=["order_id", "order_item_id"],
    business_rules=[
        "price must be greater than 0",
        "freight_value must be >= 0",
        "order_item_id must be sequential starting from 1"
    ],
    columns=[
        ColumnContract("order_id", "string", nullable=False),
        ColumnContract("order_item_id", "integer", nullable=False, min_value=1),
        ColumnContract("product_id", "string", nullable=False),
        ColumnContract("seller_id", "string", nullable=False),
        ColumnContract("shipping_limit_date", "timestamp", nullable=True),
        ColumnContract("price", "double", nullable=False, min_value=0.01, max_value=10000.0,
                      description="Item price in BRL"),
        ColumnContract("freight_value", "double", nullable=False, min_value=0.0,
                      description="Shipping cost in BRL"),
    ]
)

PAYMENTS_CONTRACT = TableContract(
    table_name="payments",
    version="1.0.0",
    owner="data_engineering",
    description="Order payments — multiple rows per order for installments",
    primary_key=["order_id", "payment_sequential"],
    business_rules=[
        "payment_value must be > 0",
        "payment_installments must be between 1 and 24",
        "Total payment_value per order should approximately equal sum of items price + freight"
    ],
    columns=[
        ColumnContract("order_id", "string", nullable=False),
        ColumnContract("payment_sequential", "integer", nullable=False, min_value=1),
        ColumnContract("payment_type", "string", nullable=False,
                      allowed_values=["credit_card", "boleto", "voucher", "debit_card", "not_defined"]),
        ColumnContract("payment_installments", "integer", nullable=False, min_value=1, max_value=24),
        ColumnContract("payment_value", "double", nullable=False, min_value=0.0),
    ]
)

REVIEWS_CONTRACT = TableContract(
    table_name="reviews",
    version="1.0.0",
    owner="data_engineering",
    description="Customer reviews — one review per order",
    primary_key=["review_id"],
    business_rules=[
        "review_score must be between 1 and 5",
        "review_creation_date must be after order delivery date",
        "Over 50% of comment fields expected to be null — this is normal"
    ],
    columns=[
        ColumnContract("review_id", "string", nullable=False),
        ColumnContract("order_id", "string", nullable=False),
        ColumnContract("review_score", "integer", nullable=False, min_value=1, max_value=5,
                      description="Customer rating 1-5 stars"),
        ColumnContract("review_comment_title", "string", nullable=True,
                      description="Comment title — expected >50% null"),
        ColumnContract("review_comment_message", "string", nullable=True,
                      description="Comment body in Portuguese — expected >50% null"),
        ColumnContract("review_creation_date", "timestamp", nullable=True),
        ColumnContract("review_answer_timestamp", "timestamp", nullable=True),
    ]
)

PRODUCTS_CONTRACT = TableContract(
    table_name="products",
    version="1.0.0",
    owner="data_engineering",
    description="Product catalog",
    primary_key=["product_id"],
    business_rules=[
        "1.8% of rows have null category — acceptable, can be dropped",
        "product_weight_g must be > 0 for physical products"
    ],
    columns=[
        ColumnContract("product_id", "string", nullable=False),
        ColumnContract("product_category_name", "string", nullable=True,
                      description="Category in Portuguese — 1.8% null acceptable"),
        ColumnContract("product_weight_g", "double", nullable=True, min_value=0.0),
        ColumnContract("product_length_cm", "double", nullable=True, min_value=0.0),
        ColumnContract("product_height_cm", "double", nullable=True, min_value=0.0),
        ColumnContract("product_width_cm", "double", nullable=True, min_value=0.0),
    ]
)

CUSTOMERS_CONTRACT = TableContract(
    table_name="customers",
    version="1.0.0",
    owner="data_engineering",
    description="Customer data — customer_id changes per order, use customer_unique_id for behavior analysis",
    primary_key=["customer_id"],
    business_rules=[
        "CRITICAL: customer_id is per-order, NOT per-customer",
        "Use customer_unique_id for repeat purchase analysis",
        "customer_state must be a valid Brazilian state abbreviation"
    ],
    columns=[
        ColumnContract("customer_id", "string", nullable=False,
                      description="Per-order ID — changes for same customer"),
        ColumnContract("customer_unique_id", "string", nullable=False,
                      description="True customer identifier — use this for behavior analysis"),
        ColumnContract("customer_zip_code_prefix", "string", nullable=False),
        ColumnContract("customer_city", "string", nullable=True),
        ColumnContract("customer_state", "string", nullable=False,
                      allowed_values=["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "GO",
                                     "ES", "PE", "CE", "PA", "MA", "MT", "MS", "PB",
                                     "RN", "AL", "PI", "SE", "RO", "AM", "TO", "AC",
                                     "AP", "RR", "DF"]),
    ]
)
SELLERS_CONTRACT = TableContract(
    table_name="sellers",
    version="1.0.0",
    owner="data_engineering",
    description="Seller catalog — 3,095 unique sellers",
    primary_key=["seller_id"],
    business_rules=[
        "Majority of sellers are in SP state",
        "No expected null values in this table"
    ],
    columns=[
        ColumnContract("seller_id", "string", nullable=False),
        ColumnContract("seller_zip_code_prefix", "string", nullable=False),
        ColumnContract("seller_city", "string", nullable=True),
        ColumnContract("seller_state", "string", nullable=False),
    ]
)

ALL_CONTRACTS: Dict[str, TableContract] = {
    "orders": ORDERS_CONTRACT,
    "items": ITEMS_CONTRACT,
    "payments": PAYMENTS_CONTRACT,
    "reviews": REVIEWS_CONTRACT,
    "products": PRODUCTS_CONTRACT,
    "customers": CUSTOMERS_CONTRACT,
    "sellers": SELLERS_CONTRACT,
}