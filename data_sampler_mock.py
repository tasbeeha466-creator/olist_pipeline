import os
import pandas as pd
import random
from datetime import datetime, timedelta
from config.local_config import DATA_DIR, SAMPLE_SIZE

def generate_mock_orders(n):
    data = []
    for i in range(n):
        order_id = f"MOCK_ORDER_{i+1}"
        customer_id = f"MOCK_CUST_{random.randint(1, 100)}"
        order_status = random.choice(["delivered", "canceled"])
        order_purchase = datetime.now() - timedelta(days=random.randint(1, 365))
        order_approved = order_purchase + timedelta(hours=random.randint(0, 48))
        order_delivered = order_purchase + timedelta(days=random.randint(2, 20))
        order_estimated = order_purchase + timedelta(days=random.randint(5, 15))
        
        data.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_status": order_status,
            "order_purchase_timestamp": order_purchase,
            "order_approved_at": order_approved,
            "order_delivered_carrier_date": order_approved + timedelta(hours=random.randint(1, 24)),
            "order_delivered_customer_date": order_delivered,
            "order_estimated_delivery_date": order_estimated,
        })
    return pd.DataFrame(data)

def generate_mock_items(n_orders):
    data = []
    order_ids = [f"MOCK_ORDER_{i+1}" for i in range(n_orders)]
    for order_id in order_ids:
        num_items = random.randint(1, 3)
        for j in range(num_items):
            data.append({
                "order_id": order_id,
                "order_item_id": j+1,
                "product_id": f"MOCK_PROD_{random.randint(1, 50)}",
                "seller_id": f"MOCK_SELL_{random.randint(1, 20)}",
                "shipping_limit_date": datetime.now(),
                "price": round(random.uniform(10, 500), 2),
                "freight_value": round(random.uniform(5, 50), 2),
            })
    return pd.DataFrame(data)

def generate_mock_customers(n):
    data = []
    for i in range(n):
        data.append({
            "customer_id": f"MOCK_CUST_{i+1}",
            "customer_unique_id": f"UNIQ_{i+1}",
            "customer_zip_code_prefix": str(random.randint(10000, 99999)),
            "customer_city": random.choice(["sao paulo", "rio de janeiro", "belo horizonte"]),
            "customer_state": random.choice(["SP", "RJ", "MG", "BA"]),
        })
    return pd.DataFrame(data)

def generate_mock_payments(n_orders):
    data = []
    order_ids = [f"MOCK_ORDER_{i+1}" for i in range(n_orders)]
    for order_id in order_ids:
        payment_type = random.choice(["credit_card", "boleto", "voucher"])
        installments = random.randint(1, 3) if payment_type == "credit_card" else 1
        data.append({
            "order_id": order_id,
            "payment_sequential": 1,
            "payment_type": payment_type,
            "payment_installments": installments,
            "payment_value": round(random.uniform(50, 1000), 2),
        })
    return pd.DataFrame(data)

def generate_mock_reviews(n_orders):
    data = []
    order_ids = [f"MOCK_ORDER_{i+1}" for i in range(n_orders)]
    for order_id in order_ids:
        review_score = random.choice([1, 2, 3, 4, 5])
        text = ""
        if review_score <= 2:
            text = random.choice(["atraso na entrega", "produto com defeito", "nao recebi", "pessimo"])
        elif review_score >= 4:
            text = random.choice(["excelente produto", "entrega rapida", "recomendo", "otimo"])
        data.append({
            "review_id": f"REV_{order_id}",
            "order_id": order_id,
            "review_score": review_score,
            "review_comment_title": text[:20],
            "review_comment_message": text,
            "review_creation_date": datetime.now(),
            "review_answer_timestamp": datetime.now(),
        })
    return pd.DataFrame(data)
def generate_mock_products():
    data = []
    for i in range(1, 51):
        data.append({
            "product_id": f"MOCK_PROD_{i}",
            "product_category_name": random.choice(["beleza_saude", "esporte_lazer", "moveis_decoracao"]),
            "product_weight_g": random.randint(100, 5000),
            "product_length_cm": random.randint(10, 100),
            "product_height_cm": random.randint(5, 50),
            "product_width_cm": random.randint(10, 80),
        })
    return pd.DataFrame(data)

def generate_mock_sellers():
    data = []
    for i in range(1, 21):
        data.append({
            "seller_id": f"MOCK_SELL_{i}",
            "seller_zip_code_prefix": str(random.randint(10000, 99999)),
            "seller_city": random.choice(["sao paulo", "rio de janeiro"]),
            "seller_state": random.choice(["SP", "RJ"]),
        })
    return pd.DataFrame(data)

def generate_mock_translation():
    return pd.DataFrame([
        {"product_category_name": "beleza_saude", "product_category_name_english": "health_beauty"},
        {"product_category_name": "esporte_lazer", "product_category_name_english": "sports_leisure"},
        {"product_category_name": "moveis_decoracao", "product_category_name_english": "furniture_decor"},
    ])

def load_and_sample():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    n_orders = SAMPLE_SIZE if SAMPLE_SIZE else 100
    
    orders = generate_mock_orders(n_orders)
    items = generate_mock_items(n_orders)
    customers = generate_mock_customers(100)
    payments = generate_mock_payments(n_orders)
    reviews = generate_mock_reviews(n_orders)
    products = generate_mock_products()
    sellers = generate_mock_sellers()
    translation = generate_mock_translation()
    
    orders.to_csv(os.path.join(DATA_DIR, "sample_orders.csv"), index=False)
    items.to_csv(os.path.join(DATA_DIR, "sample_items.csv"), index=False)
    customers.to_csv(os.path.join(DATA_DIR, "sample_customers.csv"), index=False)
    payments.to_csv(os.path.join(DATA_DIR, "sample_payments.csv"), index=False)
    reviews.to_csv(os.path.join(DATA_DIR, "sample_reviews.csv"), index=False)
    products.to_csv(os.path.join(DATA_DIR, "sample_products.csv"), index=False)
    sellers.to_csv(os.path.join(DATA_DIR, "sample_sellers.csv"), index=False)
    translation.to_csv(os.path.join(DATA_DIR, "sample_translation.csv"), index=False)
    
    print(f"Generated {n_orders} mock orders and all related tables.")
    return [f"MOCK_ORDER_{i+1}" for i in range(n_orders)]

if __name__ == "__main__":
    load_and_sample()