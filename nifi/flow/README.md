## NiFi flow

- **`../../scripts/bootstrap_nifi.sh`** — provisions **Medallion_ListenHTTP** → **Medallion_InvokeHTTP_CH** → ClickHouse `bronze.events` via the NiFi REST API (run on the host after `docker compose up`).
- `flow_definition.md` — manual processor reference (same topology as the bootstrap script).
- `flow_definition.json` — short machine-readable summary.

NiFi provides **realtime ingestion** (ListenHTTP, InvokeHTTP). **Airflow + dbt** handle batch CSV loads and all **silver/gold** transformations.