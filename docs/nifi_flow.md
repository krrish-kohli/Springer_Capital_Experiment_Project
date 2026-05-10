## NiFi — realtime path

### Automated bootstrap (recommended)

After `docker compose up -d`, run on the host:

```bash
./scripts/bootstrap_nifi.sh
```

This uses the NiFi REST API (JWT from `POST /nifi-api/access/token`) to create **Medallion_ListenHTTP** (port `8081`, base path `contentListener`) and **Medallion_InvokeHTTP_CH** (POST to ClickHouse `JSONEachRow` insert). It is idempotent: re-running ensures processors are **RUNNING** and connected.

Environment overrides: `NIFI_BASE`, `NIFI_USER`, `NIFI_PASS`, `CLICKHOUSE_HTTP_URL` (must be reachable from the **NiFi** container, default `http://clickhouse:8123`).

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

- **NiFi UI**: **Medallion_ListenHTTP** / **Medallion_InvokeHTTP_CH** are **RUNNING** and show non-zero flow where appropriate.
- **ClickHouse**: `bronze.events` row count increases while `event_sim` runs.
- **Airflow**: `medallion_dbt_every_5m` succeeds (or trigger batch DAG + dbt).
- **Warehouse outputs**: `silver.fct_events` and `gold.*` tables have rows after dbt.

See [nifi/flow/flow_definition.md](../nifi/flow/flow_definition.md) for processor details.
