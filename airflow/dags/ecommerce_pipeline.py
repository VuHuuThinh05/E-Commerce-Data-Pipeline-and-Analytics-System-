from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime, timedelta
import sys
import time
import threading

sys.path.insert(0, "/opt/airflow/scripts")

from kafka_producer import EcommerceProducer
from kafka_consumer import EcommerceConsumer


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_kafka_producer():
    """
    Stream a batch of e-commerce orders to Kafka.
    """
    producer = EcommerceProducer()

    try:
        producer.load_data()
        producer.stream_orders(
            speed_multiplier=1000,
            max_events=10000
        )
    finally:
        producer.close()


def run_kafka_consumer():
    """
    Consume Kafka messages and write them to PostgreSQL.
    """
    consumer = EcommerceConsumer()

    try:
        consumer_thread = threading.Thread(
            target=consumer.process_messages
        )

        consumer_thread.daemon = True
        consumer_thread.start()

        # Wait for the consumer to process the batch
        consumer_thread.join(timeout=60)

        # Request shutdown
        consumer.shutdown_requested = True

        # Give the consumer time to finish cleanly
        consumer_thread.join(timeout=10)

    finally:
        consumer.close()


with DAG(
    dag_id="ecommerce_daily_pipeline",

    default_args=default_args,

    description="Daily e-commerce data pipeline",

    schedule="@daily",

    catchup=False,

    tags=[
        "ecommerce",
        "kafka",
        "postgresql",
        "dbt",
        "etl"
    ],
) as dag:

    # =========================================================
    # TASK 1 — PRODUCER
    # =========================================================

    stream_orders = PythonOperator(
        task_id="stream_orders_to_kafka",
        python_callable=run_kafka_producer,
    )

    # =========================================================
    # TASK 2 — CONSUMER
    # =========================================================

    consume_orders = PythonOperator(
        task_id="consume_orders_from_kafka",
        python_callable=run_kafka_consumer,
    )

    # =========================================================
    # TASK 3 — DBT RUN
    # =========================================================

    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            "cd /opt/airflow/dbt/ecommerce_analytics "
            "&& dbt run"
        ),
    )

    # =========================================================
    # TASK 4 — DBT TEST
    # =========================================================

    test_dbt = BashOperator(
        task_id="test_dbt_models",
        bash_command=(
            "cd /opt/airflow/dbt/ecommerce_analytics "
            "&& dbt test"
        ),
    )

    # =========================================================
    # PIPELINE DEPENDENCY
    # =========================================================

    stream_orders >> consume_orders >> run_dbt >> test_dbt