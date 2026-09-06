from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2 import pool
import logging
import signal
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

KAFKA_TOPIC = os.getenv(
    'KAFKA_TOPIC',
    'ecommerce-orders'
)

KAFKA_GROUP_ID = os.getenv(
    'KAFKA_GROUP_ID',
    'ecommerce-consumer-group'
)

DB_HOST = os.getenv(
    'DB_HOST',
    'postgres'
)

DB_PORT = int(os.getenv(
    'DB_PORT',
    '5432'
))

DB_NAME = os.getenv(
    'DB_NAME',
    'ecommerce'
)

DB_USER = os.getenv(
    'DB_USER',
    'airflow'
)

DB_PASSWORD = os.getenv(
    'DB_PASSWORD',
    'airflow'
)

DB_POOL_MIN = int(os.getenv(
    'DB_POOL_MIN',
    '1'
))

DB_POOL_MAX = int(os.getenv(
    'DB_POOL_MAX',
    '5'
))

BATCH_SIZE = int(os.getenv(
    'BATCH_SIZE',
    '100'
))


# ============================================================
# Ecommerce Consumer
# ============================================================

class EcommerceConsumer:

    def __init__(self):

        self.shutdown_requested = False

        # ----------------------------------------------------
        # Set up signal handlers for graceful shutdown
        # ----------------------------------------------------

        signal.signal(
            signal.SIGINT,
            self._signal_handler
        )

        signal.signal(
            signal.SIGTERM,
            self._signal_handler
        )


        # ----------------------------------------------------
        # Connect to Kafka
        # ----------------------------------------------------

        try:

            self.consumer = KafkaConsumer(
                KAFKA_TOPIC,

                bootstrap_servers=[
                    KAFKA_BOOTSTRAP_SERVERS
                ],

                value_deserializer=lambda m:
                    json.loads(
                        m.decode('utf-8')
                    ) if m else None,

                group_id=KAFKA_GROUP_ID,

                auto_offset_reset='earliest'
            )

            logger.info(
                f"Connected to Kafka at "
                f"{KAFKA_BOOTSTRAP_SERVERS}, "
                f"topic: {KAFKA_TOPIC}"
            )

        except Exception as e:

            logger.error(
                f"Failed to connect to Kafka: {e}"
            )

            raise


        # ----------------------------------------------------
        # Create PostgreSQL connection pool
        # ----------------------------------------------------

        try:

            self.db_pool = pool.SimpleConnectionPool(

                minconn=DB_POOL_MIN,

                maxconn=DB_POOL_MAX,

                host=DB_HOST,

                port=DB_PORT,

                database=DB_NAME,

                user=DB_USER,

                password=DB_PASSWORD
            )

            logger.info(
                f"PostgreSQL connection pool created "
                f"(min={DB_POOL_MIN}, max={DB_POOL_MAX})"
            )

        except psycopg2.Error as e:

            logger.error(
                f"Failed to create PostgreSQL "
                f"connection pool: {e}"
            )

            raise


        # ----------------------------------------------------
        # Create RAW tables
        # ----------------------------------------------------

        self.create_table()


    # ========================================================
    # Signal handler
    # ========================================================

    def _signal_handler(self, signum, frame):

        logger.info(
            f"Received signal {signum}, "
            f"initiating graceful shutdown..."
        )

        self.shutdown_requested = True


    # ========================================================
    # Create RAW tables
    # ========================================================

    def create_table(self):

        conn = None

        try:

            conn = self.db_pool.getconn()

            cursor = conn.cursor()


            # ------------------------------------------------
            # raw_orders
            # ------------------------------------------------

            cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS raw;
                CREATE TABLE IF NOT EXISTS raw.raw_orders (
                    id SERIAL PRIMARY KEY,
                    order_id VARCHAR(100),
                    customer_id VARCHAR(100),
                    order_status VARCHAR(50),
                    order_purchase_timestamp TIMESTAMP,
                    order_approved_at TIMESTAMP,
                    order_delivered_carrier_date TIMESTAMP,
                    order_delivered_customer_date TIMESTAMP,
                    order_estimated_delivery_date TIMESTAMP,
                    event_type VARCHAR(50),
                    event_timestamp TIMESTAMP,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            # ------------------------------------------------
            # raw_order_items
            # ------------------------------------------------

            cursor.execute("""

                CREATE TABLE IF NOT EXISTS raw.raw_order_items (

                    id SERIAL PRIMARY KEY,

                    order_id VARCHAR(100),

                    order_item_id INTEGER,

                    product_id VARCHAR(100),

                    seller_id VARCHAR(100),

                    shipping_limit_date TIMESTAMP,

                    price DECIMAL(12, 2),

                    freight_value DECIMAL(12, 2),

                    event_type VARCHAR(50),

                    event_timestamp TIMESTAMP,

                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)


            # ------------------------------------------------
            # raw_payments
            # ------------------------------------------------

            cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS raw;
                CREATE TABLE IF NOT EXISTS raw.raw_payments (

                    id SERIAL PRIMARY KEY,

                    order_id VARCHAR(100),

                    payment_sequential INTEGER,

                    payment_type VARCHAR(50),

                    payment_installments INTEGER,

                    payment_value DECIMAL(12, 2),

                    event_type VARCHAR(50),

                    event_timestamp TIMESTAMP,

                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)


            # ------------------------------------------------
            # raw_customers
            # ------------------------------------------------

            cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS raw;
                CREATE TABLE IF NOT EXISTS raw.raw_customers (

                    id SERIAL PRIMARY KEY,

                    customer_id VARCHAR(100),

                    customer_unique_id VARCHAR(100),

                    customer_zip_code_prefix INTEGER,

                    customer_city VARCHAR(150),

                    customer_state VARCHAR(10),

                    event_type VARCHAR(50),

                    event_timestamp TIMESTAMP,

                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)


            # ------------------------------------------------
            # raw_products
            # ------------------------------------------------

            cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS raw;
                CREATE TABLE IF NOT EXISTS raw.raw_products (

                    id SERIAL PRIMARY KEY,

                    product_id VARCHAR(100),

                    product_category_name VARCHAR(150),

                    product_name_lenght INTEGER,

                    product_description_lenght INTEGER,

                    product_photos_qty INTEGER,

                    product_weight_g INTEGER,

                    product_length_cm DECIMAL(10, 2),

                    product_height_cm DECIMAL(10, 2),

                    product_width_cm DECIMAL(10, 2),

                    event_type VARCHAR(50),

                    event_timestamp TIMESTAMP,

                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)


            # ------------------------------------------------
            # raw_sellers
            # ------------------------------------------------

            cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS raw;
                CREATE TABLE IF NOT EXISTS raw.raw_sellers (

                    id SERIAL PRIMARY KEY,

                    seller_id VARCHAR(100),

                    seller_zip_code_prefix INTEGER,

                    seller_city VARCHAR(150),

                    seller_state VARCHAR(10),

                    event_type VARCHAR(50),

                    event_timestamp TIMESTAMP,

                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)


            # ------------------------------------------------
            # raw_reviews
            # ------------------------------------------------

            cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS raw;
                CREATE TABLE IF NOT EXISTS raw.raw_reviews (

                    id SERIAL PRIMARY KEY,

                    review_id VARCHAR(100),

                    order_id VARCHAR(100),

                    review_score INTEGER,

                    review_comment_title TEXT,

                    review_comment_message TEXT,

                    review_creation_date TIMESTAMP,

                    review_answer_timestamp TIMESTAMP,

                    event_type VARCHAR(50),

                    event_timestamp TIMESTAMP,

                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)


            # ------------------------------------------------
            # raw_geolocation
            # ------------------------------------------------

            cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS raw;
                CREATE TABLE IF NOT EXISTS raw.raw_geolocation (

                    id SERIAL PRIMARY KEY,

                    geolocation_zip_code_prefix INTEGER,

                    geolocation_lat DECIMAL(12, 8),

                    geolocation_lng DECIMAL(12, 8),

                    geolocation_city VARCHAR(150),

                    geolocation_state VARCHAR(10),

                    event_type VARCHAR(50),

                    event_timestamp TIMESTAMP,

                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)


            # ------------------------------------------------
            # raw_category_translation
            # ------------------------------------------------

            cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS raw;
                CREATE TABLE IF NOT EXISTS raw.raw_category_translation (

                    id SERIAL PRIMARY KEY,

                    product_category_name VARCHAR(150),

                    product_category_name_english VARCHAR(150),

                    event_type VARCHAR(50),

                    event_timestamp TIMESTAMP,

                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)


            # =================================================
            # Indexes
            # =================================================

            indexes = [

                # Orders
                """
                CREATE INDEX IF NOT EXISTS
                idx_raw_orders_order_id
                ON raw.raw_orders(order_id);
                """,

                """
                CREATE INDEX IF NOT EXISTS
                idx_raw_orders_customer_id
                ON raw.raw_orders(customer_id);
                """,

                # Order items
                """
                CREATE INDEX IF NOT EXISTS
                idx_raw_order_items_order_id
                ON raw.raw_order_items(order_id);
                """,

                """
                CREATE INDEX IF NOT EXISTS
                idx_raw_order_items_product_id
                ON raw.raw_order_items(product_id);
                """,

                # Payments
                """
                CREATE INDEX IF NOT EXISTS
                idx_raw_payments_order_id
                ON raw.raw_payments(order_id);
                """,

                # Customers
                """
                CREATE INDEX IF NOT EXISTS
                idx_raw_customers_customer_id
                ON raw.raw_customers(customer_id);
                """,

                """
                CREATE INDEX IF NOT EXISTS
                idx_raw_customers_unique_id
                ON raw.raw_customers(customer_unique_id);
                """,

                # Products
                """
                CREATE INDEX IF NOT EXISTS
                idx_raw_products_product_id
                ON raw.raw_products(product_id);
                """,

                # Sellers
                """
                CREATE INDEX IF NOT EXISTS
                idx_raw_sellers_seller_id
                ON raw.raw_sellers(seller_id);
                """,

                # Reviews
                """
                CREATE INDEX IF NOT EXISTS
                idx_raw_reviews_order_id
                ON raw.raw_reviews(order_id);
                """,

                # Geolocation
                """
                CREATE INDEX IF NOT EXISTS
                idx_raw_geolocation_zip
                ON raw.raw_geolocation(
                    geolocation_zip_code_prefix
                );
                """,

                # Category translation
                """
                CREATE INDEX IF NOT EXISTS
                idx_raw_category_translation_name
                ON raw.raw_category_translation(
                    product_category_name
                );
                """
            ]


            for index_query in indexes:

                cursor.execute(index_query)


            conn.commit()

            cursor.close()

            logger.info(
                "All RAW tables and indexes "
                "created/verified successfully"
            )


        except psycopg2.Error as e:

            logger.error(
                f"Error creating RAW tables "
                f"or indexes: {e}"
            )

            if conn:
                conn.rollback()

            raise

        finally:

            if conn:
                self.db_pool.putconn(conn)


    # ========================================================
    # Process Kafka messages
    # ========================================================

    def process_messages(self):

        logger.info(
            "Starting to consume messages..."
        )

        batch = []

        batch_size = BATCH_SIZE


        try:

            for message in self.consumer:

                # --------------------------------------------
                # Check shutdown request
                # --------------------------------------------

                if self.shutdown_requested:

                    logger.info(
                        "Shutdown requested, "
                        "processing remaining batch..."
                    )

                    break


                try:

                    event = message.value

                    batch.append(event)


                    # ----------------------------------------
                    # Process in batches
                    # ----------------------------------------

                    if len(batch) >= batch_size:

                        self.write_batch(batch)

                        batch = []


                except json.JSONDecodeError as e:

                    logger.error(
                        f"Failed to decode message: {e}"
                    )

                    continue

                except Exception as e:

                    logger.error(
                        f"Error processing message: {e}"
                    )

                    continue


        except KeyboardInterrupt:

            logger.info(
                "Stopping consumer..."
            )


        except Exception as e:

            logger.error(
                f"Unexpected error in "
                f"message consumption: {e}"
            )

            raise


        finally:

            # ----------------------------------------------
            # Process remaining messages
            # ----------------------------------------------

            if batch:

                logger.info(
                    f"Processing final batch "
                    f"of {len(batch)} messages..."
                )

                self.write_batch(batch)


    # ========================================================
    # Write batch
    # ========================================================

    def write_batch(self, batch):

        if not batch:
            return


        conn = None

        try:

            conn = self.db_pool.getconn()

            cursor = conn.cursor()


            # =================================================
            # Separate events by dataset
            # =================================================

            dataset_batches = {

                'orders': [],

                'order_items': [],

                'payments': [],

                'customers': [],

                'products': [],

                'sellers': [],

                'reviews': [],

                'geolocation': [],

                'category_translation': []
            }


            # =================================================
            # Parse events
            # =================================================

            for event in batch:

                try:

                    dataset = event.get(
                        'dataset'
                    )

                    data = event.get(
                        'data',
                        {}
                    )

                    event_type = event.get(
                        'event_type'
                    )

                    event_timestamp = event.get(
                        'event_timestamp'
                    )


                    # -----------------------------------------
                    # Validate dataset
                    # -----------------------------------------

                    if dataset not in dataset_batches:

                        logger.warning(
                            f"Unknown dataset: {dataset}"
                        )

                        continue


                    # -----------------------------------------
                    # Convert timestamp
                    # -----------------------------------------

                    timestamp = None

                    if event_timestamp:

                        try:

                            timestamp = datetime.fromisoformat(
                                event_timestamp
                            )

                        except ValueError:

                            logger.warning(
                                f"Invalid event timestamp: "
                                f"{event_timestamp}"
                            )


                    # =================================================
                    # ORDERS
                    # =================================================

                    if dataset == 'orders':

                        dataset_batches[
                            'orders'
                        ].append((

                            data.get('order_id'),

                            data.get('customer_id'),

                            data.get('order_status'),

                            self.parse_timestamp(
                                data.get(
                                    'order_purchase_timestamp'
                                )
                            ),

                            self.parse_timestamp(
                                data.get(
                                    'order_approved_at'
                                )
                            ),

                            self.parse_timestamp(
                                data.get(
                                    'order_delivered_carrier_date'
                                )
                            ),

                            self.parse_timestamp(
                                data.get(
                                    'order_delivered_customer_date'
                                )
                            ),

                            self.parse_timestamp(
                                data.get(
                                    'order_estimated_delivery_date'
                                )
                            ),

                            event_type,

                            timestamp
                        ))


                    # =================================================
                    # ORDER ITEMS
                    # =================================================

                    elif dataset == 'order_items':

                        dataset_batches[
                            'order_items'
                        ].append((

                            data.get('order_id'),

                            self.safe_int(
                                data.get(
                                    'order_item_id'
                                )
                            ),

                            data.get('product_id'),

                            data.get('seller_id'),

                            self.parse_timestamp(
                                data.get(
                                    'shipping_limit_date'
                                )
                            ),

                            self.safe_decimal(
                                data.get('price')
                            ),

                            self.safe_decimal(
                                data.get(
                                    'freight_value'
                                )
                            ),

                            event_type,

                            timestamp
                        ))


                    # =================================================
                    # PAYMENTS
                    # =================================================

                    elif dataset == 'payments':

                        dataset_batches[
                            'payments'
                        ].append((

                            data.get('order_id'),

                            self.safe_int(
                                data.get(
                                    'payment_sequential'
                                )
                            ),

                            data.get('payment_type'),

                            self.safe_int(
                                data.get(
                                    'payment_installments'
                                )
                            ),

                            self.safe_decimal(
                                data.get(
                                    'payment_value'
                                )
                            ),

                            event_type,

                            timestamp
                        ))


                    # =================================================
                    # CUSTOMERS
                    # =================================================

                    elif dataset == 'customers':

                        dataset_batches[
                            'customers'
                        ].append((

                            data.get('customer_id'),

                            data.get(
                                'customer_unique_id'
                            ),

                            self.safe_int(
                                data.get(
                                    'customer_zip_code_prefix'
                                )
                            ),

                            data.get('customer_city'),

                            data.get('customer_state'),

                            event_type,

                            timestamp
                        ))


                    # =================================================
                    # PRODUCTS
                    # =================================================

                    elif dataset == 'products':

                        dataset_batches[
                            'products'
                        ].append((

                            data.get('product_id'),

                            data.get(
                                'product_category_name'
                            ),

                            self.safe_int(
                                data.get(
                                    'product_name_lenght'
                                )
                            ),

                            self.safe_int(
                                data.get(
                                    'product_description_lenght'
                                )
                            ),

                            self.safe_int(
                                data.get(
                                    'product_photos_qty'
                                )
                            ),

                            self.safe_int(
                                data.get(
                                    'product_weight_g'
                                )
                            ),

                            self.safe_decimal(
                                data.get(
                                    'product_length_cm'
                                )
                            ),

                            self.safe_decimal(
                                data.get(
                                    'product_height_cm'
                                )
                            ),

                            self.safe_decimal(
                                data.get(
                                    'product_width_cm'
                                )
                            ),

                            event_type,

                            timestamp
                        ))


                    # =================================================
                    # SELLERS
                    # =================================================

                    elif dataset == 'sellers':

                        dataset_batches[
                            'sellers'
                        ].append((

                            data.get('seller_id'),

                            self.safe_int(
                                data.get(
                                    'seller_zip_code_prefix'
                                )
                            ),

                            data.get('seller_city'),

                            data.get('seller_state'),

                            event_type,

                            timestamp
                        ))


                    # =================================================
                    # REVIEWS
                    # =================================================

                    elif dataset == 'reviews':

                        dataset_batches[
                            'reviews'
                        ].append((

                            data.get('review_id'),

                            data.get('order_id'),

                            self.safe_int(
                                data.get(
                                    'review_score'
                                )
                            ),

                            data.get(
                                'review_comment_title'
                            ),

                            data.get(
                                'review_comment_message'
                            ),

                            self.parse_timestamp(
                                data.get(
                                    'review_creation_date'
                                )
                            ),

                            self.parse_timestamp(
                                data.get(
                                    'review_answer_timestamp'
                                )
                            ),

                            event_type,

                            timestamp
                        ))


                    # =================================================
                    # GEOLOCATION
                    # =================================================

                    elif dataset == 'geolocation':

                        dataset_batches[
                            'geolocation'
                        ].append((

                            self.safe_int(
                                data.get(
                                    'geolocation_zip_code_prefix'
                                )
                            ),

                            self.safe_decimal(
                                data.get(
                                    'geolocation_lat'
                                )
                            ),

                            self.safe_decimal(
                                data.get(
                                    'geolocation_lng'
                                )
                            ),

                            data.get(
                                'geolocation_city'
                            ),

                            data.get(
                                'geolocation_state'
                            ),

                            event_type,

                            timestamp
                        ))


                    # =================================================
                    # CATEGORY TRANSLATION
                    # =================================================

                    elif dataset == 'category_translation':

                        dataset_batches[
                            'category_translation'
                        ].append((

                            data.get(
                                'product_category_name'
                            ),

                            data.get(
                                'product_category_name_english'
                            ),

                            event_type,

                            timestamp
                        ))


                except Exception as e:

                    logger.error(
                        f"Error parsing event: {e}"
                    )

                    continue


            # =================================================
            # Insert ORDERS
            # =================================================

            if dataset_batches['orders']:

                cursor.executemany(
                    """
                    INSERT INTO raw.raw_orders (
                        order_id,
                        customer_id,
                        order_status,
                        order_purchase_timestamp,
                        order_approved_at,
                        order_delivered_carrier_date,
                        order_delivered_customer_date,
                        order_estimated_delivery_date,
                        event_type,
                        event_timestamp
                    )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    """,
                    dataset_batches['orders']
                )


            # =================================================
            # Insert ORDER ITEMS
            # =================================================

            if dataset_batches['order_items']:

                cursor.executemany(
                    """
                    INSERT INTO raw.raw_order_items (
                        order_id,
                        order_item_id,
                        product_id,
                        seller_id,
                        shipping_limit_date,
                        price,
                        freight_value,
                        event_type,
                        event_timestamp
                    )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    """,
                    dataset_batches['order_items']
                )


            # =================================================
            # Insert PAYMENTS
            # =================================================

            if dataset_batches['payments']:

                cursor.executemany(
                    """
                    INSERT INTO raw.raw_payments (
                        order_id,
                        payment_sequential,
                        payment_type,
                        payment_installments,
                        payment_value,
                        event_type,
                        event_timestamp
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    dataset_batches['payments']
                )


            # =================================================
            # Insert CUSTOMERS
            # =================================================

            if dataset_batches['customers']:

                cursor.executemany(
                    """
                    INSERT INTO raw.raw_customers (
                        customer_id,
                        customer_unique_id,
                        customer_zip_code_prefix,
                        customer_city,
                        customer_state,
                        event_type,
                        event_timestamp
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    dataset_batches['customers']
                )


            # =================================================
            # Insert PRODUCTS
            # =================================================

            if dataset_batches['products']:

                cursor.executemany(
                    """
                    INSERT INTO raw.raw_products (
                        product_id,
                        product_category_name,
                        product_name_lenght,
                        product_description_lenght,
                        product_photos_qty,
                        product_weight_g,
                        product_length_cm,
                        product_height_cm,
                        product_width_cm,
                        event_type,
                        event_timestamp
                    )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    )
                    """,
                    dataset_batches['products']
                )


            # =================================================
            # Insert SELLERS
            # =================================================

            if dataset_batches['sellers']:

                cursor.executemany(
                    """
                    INSERT INTO raw.raw_sellers (
                        seller_id,
                        seller_zip_code_prefix,
                        seller_city,
                        seller_state,
                        event_type,
                        event_timestamp
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s
                    )
                    """,
                    dataset_batches['sellers']
                )


            # =================================================
            # Insert REVIEWS
            # =================================================

            if dataset_batches['reviews']:

                cursor.executemany(
                    """
                    INSERT INTO raw.raw_reviews (
                        review_id,
                        order_id,
                        review_score,
                        review_comment_title,
                        review_comment_message,
                        review_creation_date,
                        review_answer_timestamp,
                        event_type,
                        event_timestamp
                    )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    """,
                    dataset_batches['reviews']
                )


            # =================================================
            # Insert GEOLOCATION
            # =================================================

            if dataset_batches['geolocation']:

                cursor.executemany(
                    """
                    INSERT INTO raw.raw_geolocation (
                        geolocation_zip_code_prefix,
                        geolocation_lat,
                        geolocation_lng,
                        geolocation_city,
                        geolocation_state,
                        event_type,
                        event_timestamp
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    dataset_batches['geolocation']
                )


            # =================================================
            # Insert CATEGORY TRANSLATION
            # =================================================

            if dataset_batches['category_translation']:

                cursor.executemany(
                    """
                    INSERT INTO raw.raw_category_translation (
                        product_category_name,
                        product_category_name_english,
                        event_type,
                        event_timestamp
                    )
                    VALUES (
                        %s, %s, %s, %s
                    )
                    """,
                    dataset_batches['category_translation']
                )


            # =================================================
            # Commit entire batch
            # =================================================

            conn.commit()


            # =================================================
            # Logging
            # =================================================

            total_written = sum(
                len(records)
                for records in dataset_batches.values()
            )

            logger.info(
                f"Wrote {total_written} events to PostgreSQL"
            )

            for dataset, records in dataset_batches.items():

                if records:

                    logger.info(
                        f"  {dataset}: "
                        f"{len(records)} records"
                    )


            cursor.close()


        except psycopg2.Error as e:

            logger.error(
                f"Database error writing batch: {e}"
            )

            if conn:
                conn.rollback()

            raise


        except Exception as e:

            logger.error(
                f"Unexpected error writing batch: {e}"
            )

            if conn:
                conn.rollback()

            raise


        finally:

            if conn:

                self.db_pool.putconn(conn)


    # ========================================================
    # Safe integer conversion
    # ========================================================

    @staticmethod
    def safe_int(value):

        if value is None:
            return None

        try:

            if isinstance(value, float) and value != value:
                return None

            return int(value)

        except (ValueError, TypeError):

            return None


    # ========================================================
    # Safe decimal conversion
    # ========================================================

    @staticmethod
    def safe_decimal(value):

        if value is None:
            return None

        try:

            if isinstance(value, float) and value != value:
                return None

            return float(value)

        except (ValueError, TypeError):

            return None


    # ========================================================
    # Timestamp conversion
    # ========================================================

    @staticmethod
    def parse_timestamp(value):

        if value is None:
            return None

        try:

            if pd_isna(value):
                return None

        except Exception:

            pass

        try:

            return datetime.fromisoformat(
                str(value).replace(
                    'Z',
                    '+00:00'
                )
            )

        except (ValueError, TypeError):

            return None


    # ========================================================
    # Close connections
    # ========================================================

    def close(self):

        logger.info(
            "Closing consumer connections..."
        )

        try:

            self.consumer.close()

            logger.info(
                "Kafka consumer closed"
            )

        except Exception as e:

            logger.error(
                f"Error closing Kafka consumer: {e}"
            )


        try:

            self.db_pool.closeall()

            logger.info(
                "PostgreSQL connection pool closed"
            )

        except Exception as e:

            logger.error(
                f"Error closing database pool: {e}"
            )


# ============================================================
# Small helper for NaN detection
# ============================================================

def pd_isna(value):

    try:

        return value != value

    except Exception:

        return False


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    consumer = EcommerceConsumer()

    try:

        consumer.process_messages()

    finally:

        consumer.close()