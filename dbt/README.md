# dbt — `medallion_demo`

ClickHouse **silver** and **gold** models for the Medallion demo.

- **Project**: [`medallion_demo/`](medallion_demo/)
- **Profile example**: [`medallion_demo/profiles.example.yml`](medallion_demo/profiles.example.yml)

Local run (ClickHouse up, env vars set):

```bash
cp medallion_demo/profiles.example.yml ~/.dbt/profiles.yml
# edit profile / env: CLICKHOUSE_HOST, etc.
cd medallion_demo && dbt build
```

In Docker, Airflow generates `profiles.yml` under `/opt/airflow/dbt/profiles` and runs `dbt build`.
