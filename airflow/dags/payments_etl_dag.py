"""Payments ETL: ingest S3 JSON into bronze, then transform to silver and gold."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/softgame"

default_args = {
    "owner": "softgame",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="payments_etl",
    default_args=default_args,
    description="Ingest payment events from S3, transform bronze -> silver -> gold",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["payments", "etl"],
) as dag:
    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"python {PROJECT_ROOT}/ingestion/main.py",
    )

    transform_silver_gold = BashOperator(
        task_id="transform_silver_gold",
        bash_command=f"python {PROJECT_ROOT}/transformation/main.py",
    )

    ingest_bronze >> transform_silver_gold
