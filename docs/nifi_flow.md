## NiFi — realtime path

### Role

- Accept HTTP POSTs with JSON event payloads (from `event_sim` with `EVENT_TARGET=nifi`, or any client).
- Optionally enrich with `ingest_ts` / `ingest_source`.
- **InvokeHTTP** to ClickHouse HTTP interface: `INSERT INTO bronze.events ... FORMAT JSONEachRow`.

### Ports

| Service | Port | Purpose |
|---------|------|---------|
| NiFi UI | 8443 (HTTPS) | Build and monitor flows (`admin` / `adminadminadmin` in local compose) |
| ListenHTTP | 8081 | Ingest URL base `http://nifi:8081/contentListener` (inside Docker network) |

### Relationship to Airflow

- NiFi **does not** run dbt.
- After events land in bronze, **Airflow** DAG `medallion_dbt_every_5m` (or a manual **Trigger DAG** on the batch DAG after CSV load) runs `dbt build` so `silver.fct_events` and gold marts refresh.

### Proof (what to check in a demo)

- **NiFi UI**: your flow is running and processors show incoming/outgoing counts.\n+- **ClickHouse**: `bronze.events` row count increases.\n+- **Airflow**: `medallion_dbt_every_5m` succeeds.\n+- **Warehouse outputs**: `silver.fct_events` and `gold.*` tables have rows.

See [nifi/flow/flow_definition.md](../nifi/flow/flow_definition.md) for processor details.
