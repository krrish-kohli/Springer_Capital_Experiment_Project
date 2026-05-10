# Troubleshooting (advanced)

This page documents **troubleshooting-only** shortcuts. The intended architecture is:

`event_sim → NiFi → ClickHouse bronze → dbt → silver/gold`

## 1) Bypass NiFi (troubleshooting only)

If you are debugging ClickHouse connectivity or NiFi is temporarily unavailable, you can have `event_sim` write directly to ClickHouse.

In `docker-compose.yml`, set:

- `EVENT_TARGET=clickhouse`

Then restart:

```bash
docker compose up -d
```

This mode is **not** the intended realtime architecture. Use it only to isolate connectivity issues.

## 2) Confirm NiFi is ingesting

- Run `./scripts/bootstrap_nifi.sh` after `docker compose up` if you have not already provisioned the Medallion ListenHTTP flow.
- In the NiFi UI, processors **Medallion_ListenHTTP** and **Medallion_InvokeHTTP_CH** should be **RUNNING** without repeated ERROR bulletins.
- In ClickHouse, `SELECT count() FROM bronze.events` should increase over time while `event_sim` is running with `EVENT_TARGET=nifi`.

## 3) Confirm dbt is refreshing silver/gold

- Airflow DAG `medallion_dbt_every_5m` should succeed (or trigger `medallion_batch_csv_to_gold` once for batch bronze + dbt).
- Tables `silver.fct_events` and `gold.*` should exist after a successful `dbt build`.

## 4) Airflow / dbt profiles on Linux

If the scheduler or webserver cannot write to `./airflow/logs`, set your host user id before starting:

```bash
export AIRFLOW_UID="$(id -u)"
docker compose up -d
```

## 5) NiFi ListenHTTP port conflict

Only one processor may listen on a given port. The bootstrap script removes **other** ListenHTTP processors on port **8081** (except **Medallion_ListenHTTP**). For a completely clean NiFi canvas you can stop the stack and remove the local state directory `nifi/state/` (not tracked in git), then start again.
