import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration from environment variables or defaults
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'ecommerce')
DB_USER = os.getenv('DB_USER', 'airflow')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')
DATA_PATH = os.getenv('DATA_PATH', 'data/raw')

# Create SQLAlchemy connection string
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Mapping CSV files to table names
DATASETS = {
    'olist_customers_dataset.csv': 'customers',
    'olist_orders_dataset.csv': 'orders',
    'olist_order_items_dataset.csv': 'order_items',
    'olist_order_payments_dataset.csv': 'payments',
    'olist_products_dataset.csv': 'products',
    'olist_sellers_dataset.csv': 'sellers',
    'olist_order_reviews_dataset.csv': 'reviews',
    'olist_geolocation_dataset.csv': 'geolocation',
    'product_category_name_translation.csv': 'category_translation'
}

def ingest_data():
    engine = create_engine(DATABASE_URL)
    
    # 1. Ensure schema 'raw' exists
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.commit()
        logger.info("Schema 'raw' verified/created.")

    # 2. Loop through all 9 CSV files and load into Postgres
    for csv_file, table_name in DATASETS.items():
        file_path = os.path.join(DATA_PATH, csv_file)
        
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}, skipping...")
            continue
            
        logger.info(f"Ingesting {csv_file} -> raw.{table_name}...")
        df = pd.read_csv(file_path)
        
        # Write dataframe directly to PostgreSQL schema 'raw'
        df.to_sql(
            name=table_name,
            con=engine,
            schema='raw',
            if_exists='replace',
            index=False
        )
        logger.info(f"Successfully loaded {len(df)} rows into raw.{table_name}")

if __name__ == "__main__":
    ingest_data()