"""
Baserow API → ClickHouse bronze.baserow_feedback_raw → dbt silver/gold (scoped build).
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import clickhouse_connect
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "medallion-demo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

DBT_PROJECT = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/medallion_demo")
DBT_PROFILES = os.environ.get("DBT_PROFILES_DIR", "/tmp/dbt_profiles")


def _ch_client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
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


def _parse_feedback_date(val: Any) -> date:
    if val is None or val == "":
        return date(1970, 1, 1)
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    s = str(val).strip()
    if not s:
        return date(1970, 1, 1)
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return date(1970, 1, 1)


def fetch_baserow_rows() -> list[dict[str, Any]]:
    token = os.environ.get("BASEROW_API_TOKEN", "").strip()
    table_id = os.environ.get("BASEROW_TABLE_ID", "").strip()
    base = os.environ.get("BASEROW_BASE_URL", "https://api.baserow.io").rstrip("/")
    if not token:
        raise ValueError("BASEROW_API_TOKEN is not set (add it to .env for docker compose).")
    if not table_id:
        raise ValueError("BASEROW_TABLE_ID is not set.")

    headers = {
        "Authorization": f"Token {token}",
        "Accept": "application/json",
    }
    url: str | None = f"{base}/api/database/rows/table/{table_id}/"
    first_params = {"user_field_names": "true"}
    out: list[dict[str, Any]] = []
    while url:
        if first_params is not None:
            resp = requests.get(url, headers=headers, params=first_params, timeout=120)
            first_params = None
        else:
            resp = requests.get(url, headers=headers, timeout=120)
        resp.raise_for_status()
        payload = resp.json()
        out.extend(payload.get("results") or [])
        nxt = payload.get("next")
        url = nxt.strip() if isinstance(nxt, str) and nxt.strip() else None
    return out


def load_baserow_to_clickhouse(**context):
    ti = context["ti"]
    rows_in = ti.xcom_pull(task_ids="fetch_baserow_rows")
    if rows_in is None:
        raise RuntimeError("Missing XCom from fetch_baserow_rows")
    ingest_ts = datetime.now(timezone.utc)
    ch_rows: list[list[Any]] = []
    for row in rows_in:
        raw_s = json.dumps(row, ensure_ascii=False, default=str)
        rid = int(row.get("id", 0))
        ch_rows.append(
            [
                rid,
                str(row.get("feedback_label") or ""),
                _parse_feedback_date(row.get("feedback_date")),
                str(row.get("rating") if row.get("rating") is not None else ""),
                str(row.get("feedback_text") or ""),
                str(row.get("feedback_id") or ""),
                str(row.get("customer_id") or ""),
                str(row.get("customer_name") or ""),
                str(row.get("product_name") or ""),
                str(row.get("region") or ""),
                ingest_ts,
                raw_s,
            ]
        )
    client = _ch_client()
    client.command("TRUNCATE TABLE IF EXISTS bronze.baserow_feedback_raw")
    if not ch_rows:
        return 0
    client.insert(
        "bronze.baserow_feedback_raw",
        ch_rows,
        column_names=[
            "id",
            "feedback_label",
            "feedback_date",
            "rating",
            "feedback_text",
            "feedback_id",
            "customer_id",
            "customer_name",
            "product_name",
            "region",
            "ingest_ts",
            "_raw",
        ],
    )
    return len(ch_rows)


with DAG(
    dag_id="baserow_feedback_to_gold",
    default_args=DEFAULT_ARGS,
    description="Baserow feedback → bronze → dbt (Baserow lineage only)",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["medallion", "baserow", "clickhouse", "dbt"],
) as dag:
    fetch_rows = PythonOperator(
        task_id="fetch_baserow_rows",
        python_callable=fetch_baserow_rows,
    )

    load_bronze = PythonOperator(
        task_id="load_baserow_to_clickhouse",
        python_callable=load_baserow_to_clickhouse,
    )

    write_profiles = PythonOperator(
        task_id="ensure_dbt_profiles",
        python_callable=ensure_dbt_profiles,
    )

    dbt_build = BashOperator(
        task_id="dbt_build_baserow",
        bash_command=f"""
set -euo pipefail
mkdir -p /tmp/dbt_target_baserow /tmp/dbt_logs_baserow
dbt build --select source:bronze.baserow_feedback_raw+ \\
  --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROFILES} \\
  --target-path /tmp/dbt_target_baserow --log-path /tmp/dbt_logs_baserow
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

    fetch_rows >> load_bronze >> write_profiles >> dbt_build
