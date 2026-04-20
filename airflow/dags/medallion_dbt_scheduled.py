"""
Periodic dbt build so silver/gold pick up new bronze.events (NiFi) without re-running CSV loads.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "medallion-demo",
    "depends_on_past": False,
}

DBT_PROJECT = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/medallion_demo")
DBT_PROFILES = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt/profiles")


def ensure_dbt_profiles():
    from pathlib import Path

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
    dag_id="medallion_dbt_every_5m",
    default_args=DEFAULT_ARGS,
    description="Run dbt build periodically for streaming + batch silver/gold",
    schedule=timedelta(minutes=5),
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["medallion", "dbt", "clickhouse"],
) as dag:
    write_profiles = PythonOperator(
        task_id="ensure_dbt_profiles",
        python_callable=ensure_dbt_profiles,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"""
set -euo pipefail
mkdir -p /tmp/dbt_target_sched /tmp/dbt_logs_sched
dbt build --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROFILES} --target-path /tmp/dbt_target_sched --log-path /tmp/dbt_logs_sched
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

    write_profiles >> dbt_build
