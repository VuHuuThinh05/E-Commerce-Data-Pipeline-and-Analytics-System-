import pandas as pd
from kafka import KafkaProducer
import json
import time
import logging
import os
from datetime import datetime

# ============================================================
# Configure logging
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================
# Configuration from environment variables
# ============================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    'KAFKA_BOOTSTRAP_SERVERS',
    'kafka:9092'
)

DATA_PATH = os.getenv(
    'DATA_PATH',
    'data/raw'
)

# ============================================================
# Ecommerce Producer
# ============================================================
class EcommerceProducer:

    def __init__(self, bootstrap_servers=None):
        if bootstrap_servers is None:
            bootstrap_servers = [KAFKA_BOOTSTRAP_SERVERS]

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(
                    v,
                    allow_nan=False
                ).encode('utf-8')
            )
            logger.info(
                f"Connected to Kafka at {bootstrap_servers}"
            )
        except Exception as e:
            logger.error(
                f"Failed to connect to Kafka: {e}"
            )
            raise

    # ========================================================
    # Load 9 Olist datasets
    # ========================================================
    def load_data(self):
        try:
            logger.info("Loading Olist datasets...")

            self.orders = pd.read_csv(
                f'{DATA_PATH}/olist_orders_dataset.csv'
            )
            self.order_items = pd.read_csv(
                f'{DATA_PATH}/olist_order_items_dataset.csv'
            )
            self.payments = pd.read_csv(
                f'{DATA_PATH}/olist_order_payments_dataset.csv'
            )
            self.customers = pd.read_csv(
                f'{DATA_PATH}/olist_customers_dataset.csv'
            )
            self.products = pd.read_csv(
                f'{DATA_PATH}/olist_products_dataset.csv'
            )
            self.sellers = pd.read_csv(
                f'{DATA_PATH}/olist_sellers_dataset.csv'
            )
            self.reviews = pd.read_csv(
                f'{DATA_PATH}/olist_order_reviews_dataset.csv'
            )
            self.geolocation = pd.read_csv(
                f'{DATA_PATH}/olist_geolocation_dataset.csv'
            )
            self.category_translation = pd.read_csv(
                f'{DATA_PATH}/product_category_name_translation.csv'
            )

        except FileNotFoundError as e:
            logger.error(
                f"Dataset file not found: {e}"
            )
            raise
        except pd.errors.ParserError as e:
            logger.error(
                f"Error parsing CSV file: {e}"
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error loading datasets: {e}"
            )
            raise

        # ====================================================
        # Keep datasets separated
        # ====================================================
        self.datasets = {
            'orders': self.orders,
            'order_items': self.order_items,
            'payments': self.payments,
            'customers': self.customers,
            'products': self.products,
            'sellers': self.sellers,
            'reviews': self.reviews,
            'geolocation': self.geolocation,
            'category_translation': self.category_translation
        }

        logger.info("Successfully loaded 9 datasets")
        for name, dataframe in self.datasets.items():
            logger.info(
                f"{name}: {len(dataframe)} records"
            )

    # ========================================================
    # Convert pandas values to JSON-safe values
    # ========================================================
    def clean_value(self, value):
        if pd.isna(value):
            return None

        # Convert pandas/numpy numeric values
        if hasattr(value, 'item'):
            try:
                return value.item()
            except Exception:
                pass

        # Convert pandas Timestamp
        if isinstance(value, pd.Timestamp):
            return value.isoformat()

        return value

    # ========================================================
    # Stream datasets to Kafka
    # ========================================================
    def stream_orders(
        self,
        speed_multiplier=5000,
        max_events=None
    ):
        logger.info(
            f"Starting to stream datasets "
            f"(speed: {speed_multiplier}x)..."
        )
        events_streamed = 0

        for dataset_name, dataframe in self.datasets.items():

            logger.info(
                f"Streaming dataset: {dataset_name} "
                f"({len(dataframe)} records)"
            )

            # Đọc theo itertuples để tiết kiệm RAM tối đa, tránh bị ngắt do OOM
            columns = list(dataframe.columns)
            
            for row in dataframe.itertuples(index=False, name=None):
                if (
                    max_events is not None
                    and events_streamed >= max_events
                ):
                    logger.info(f"Reached max_events={max_events}")
                    self.producer.flush()
                    return

                try:
                    # Tạo dictionary thủ công từ tuple để không tốn RAM
                    row_dict = dict(zip(columns, row))

                    clean_row = {
                        key: self.clean_value(value)
                        for key, value in row_dict.items()
                    }

                    event = {
                        'dataset': dataset_name,
                        'event_type': f'{dataset_name}_record',
                        'event_timestamp': datetime.now().isoformat(),
                        'data': clean_row
                    }

                    self.producer.send(
                        'ecommerce-orders',
                        value=event
                    )

                    events_streamed += 1

                    if events_streamed % 10000 == 0:
                        logger.info(f"Streamed {events_streamed} events...")
                        self.producer.flush()

                except Exception as e:
                    logger.error(
                        f"Error streaming {dataset_name} event: {e}"
                    )
                    continue

        self.producer.flush()
        logger.info(f"Finished streaming {events_streamed} events")

    # ========================================================
    # Close producer
    # ========================================================
    def close(self):
        self.producer.close()


# ============================================================
# Main
# ============================================================
if __name__ == "__main__":

    producer = EcommerceProducer()

    try:
        producer.load_data()

        producer.stream_orders(
            speed_multiplier=5000,
            max_events=None
        )

    finally:
        producer.close()