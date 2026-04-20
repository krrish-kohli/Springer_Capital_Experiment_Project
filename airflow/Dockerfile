FROM apache/airflow:2.10.3-python3.12

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends git \
  && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    "dbt-core==1.8.7" \
    "dbt-clickhouse==1.8.1" \
    "clickhouse-connect==0.8.3"
