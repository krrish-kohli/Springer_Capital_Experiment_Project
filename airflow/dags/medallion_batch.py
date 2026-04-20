"""
Batch pipeline: CSV → ClickHouse bronze → dbt silver/gold.
Schedule: manual first run friendly; set to hourly for demos as needed.
"""
from __future__ import annotations

import csv
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import clickhouse_connect
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "medallion-demo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

DATA_DIR = Path(os.environ.get("BATCH_DATA_DIR", "/opt/airflow/data/batch"))
DBT_PROJECT = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/medallion_demo")
DBT_PROFILES = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt/profiles")


def _ch_client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


def truncate_batch_bronze_tables():
    client = _ch_client()
    client.command("TRUNCATE TABLE IF EXISTS bronze.customers_raw")
    client.command("TRUNCATE TABLE IF EXISTS bronze.orders_raw")


def load_customers_to_bronze():
    path = DATA_DIR / "customers.csv"
    if not path.exists():
        raise FileNotFoundError(path)
    load_id = str(uuid.uuid4())
    file_name = path.name
    loaded_at = datetime.now(timezone.utc)
    rows = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(
                [
                    load_id,
                    file_name,
                    loaded_at,
                    r["customer_id"].strip(),
                    r["email"].strip(),
                    r["country_code"].strip(),
                    r["created_at"].strip(),
                    r["signup_date"].strip(),
                ]
            )
    client = _ch_client()
    client.insert(
        "bronze.customers_raw",
        rows,
        column_names=[
            "load_id",
            "file_name",
            "loaded_at",
            "customer_id",
            "email",
            "country_code",
            "created_at",
            "signup_date",
        ],
    )


def load_orders_to_bronze():
    path = DATA_DIR / "orders.csv"
    if not path.exists():
        raise FileNotFoundError(path)
    load_id = str(uuid.uuid4())
    file_name = path.name
    loaded_at = datetime.now(timezone.utc)
    rows = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(
                [
                    load_id,
                    file_name,
                    loaded_at,
                    r["order_id"].strip(),
                    r["customer_id"].strip(),
                    r["order_total"].strip(),
                    r["order_ts"].strip(),
                    r["currency"].strip(),
                ]
            )
    client = _ch_client()
    client.insert(
        "bronze.orders_raw",
        rows,
        column_names=[
            "load_id",
            "file_name",
            "loaded_at",
            "order_id",
            "customer_id",
            "order_total",
            "order_ts",
            "currency",
        ],
    )


def ensure_dbt_profiles():
    DBT_PROFILES_PATH = Path(DBT_PROFILES)
    DBT_PROFILES_PATH.mkdir(parents=True, exist_ok=True)
    profiles = DBT_PROFILES_PATH / "profiles.yml"
    profiles.write_text(
        "\n".join(
            [
                "medallion_demo:",
                "  target: dev",
                "  outputs:",
                "    dev:",
                "      type: clickhouse",
                "      schema: default",
                f"      host: \"{os.environ.get('CLICKHOUSE_HOST', 'clickhouse')}\"",
                "      port: 8123",
                f"      user: \"{os.environ.get('CLICKHOUSE_USER', 'default')}\"",
                f"      password: \"{os.environ.get('CLICKHOUSE_PASSWORD', '')}\"",
                "      secure: false",
                "      verify: false",
                f"      database: \"{os.environ.get('CLICKHOUSE_DATABASE', 'default')}\"",
                "",
            ]
        ),
        encoding="utf-8",
    )


with DAG(
    dag_id="medallion_batch_csv_to_gold",
    default_args=DEFAULT_ARGS,
    description="Load batch CSVs into bronze, then run dbt",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["medallion", "batch", "clickhouse", "dbt"],
) as dag:
    truncate_bronze = PythonOperator(
        task_id="truncate_batch_bronze",
        python_callable=truncate_batch_bronze_tables,
    )

    load_customers = PythonOperator(
        task_id="load_customers_csv",
        python_callable=load_customers_to_bronze,
    )
    load_orders = PythonOperator(
        task_id="load_orders_csv",
        python_callable=load_orders_to_bronze,
    )

    write_profiles = PythonOperator(
        task_id="ensure_dbt_profiles",
        python_callable=ensure_dbt_profiles,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"""
set -euo pipefail
mkdir -p /tmp/dbt_target /tmp/dbt_logs
dbt build --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROFILES} --target-path /tmp/dbt_target --log-path /tmp/dbt_logs
""",
        env={
            **os.environ,
            "CLICKHOUSE_HOST": os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            "CLICKHOUSE_PORT": os.environ.get("CLICKHOUSE_PORT", "8123"),
            "CLICKHOUSE_USER": os.environ.get("CLICKHOUSE_USER", "default"),
            "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD", ""),
            "CLICKHOUSE_DATABASE": os.environ.get("CLICKHOUSE_DATABASE", "default"),
        },
    )

    truncate_bronze >> [load_customers, load_orders] >> write_profiles >> dbt_build
