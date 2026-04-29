import os
import pandas as pd
from config.local_config import DATA_DIR, KAGGLE_DATASET, SAMPLE_SIZE

def download_dataset():
    import kaggle
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(
        KAGGLE_DATASET,
        path=DATA_DIR,
        unzip=True
    )

def load_and_sample():
    orders = pd.read_csv(os.path.join(DATA_DIR, "sample_orders.csv"))
    items = pd.read_csv(os.path.join(DATA_DIR, "sample_items.csv"))
    customers = pd.read_csv(os.path.join(DATA_DIR, "sample_customers.csv"))
    payments = pd.read_csv(os.path.join(DATA_DIR, "sample_payments.csv"))
    reviews = pd.read_csv(os.path.join(DATA_DIR, "sample_reviews.csv"))
    products = pd.read_csv(os.path.join(DATA_DIR, "sample_products.csv"))
    sellers = pd.read_csv(os.path.join(DATA_DIR, "sample_sellers.csv"))
    translation = pd.read_csv(os.path.join(DATA_DIR, "sample_translation.csv"))

    print(f"Loaded {len(orders)} orders from existing sample files.")
    print("Sample data ready.")
    