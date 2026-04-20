## Architecture — Medallion on ClickHouse

### Layers (ClickHouse databases)

| Layer | Contents | Written by |
|-------|----------|------------|
| **bronze** | Raw events (`events`), raw CSV landing (`customers_raw`, `orders_raw`) | NiFi / `event_sim`, Airflow Python tasks |
| **silver** | Conformed facts/dims: `fct_events`, `dim_customer`, `fct_orders` | dbt |
| **gold** | Analytics marts: `mart_funnel_daily`, `mart_sales_daily`, `mart_product_popularity` | dbt |

### End-to-end flows

**Realtime**

1. `event_sim` generates JSON user events and POSTs them to **NiFi** (**ListenHTTP**).
2. **NiFi** lands events in **`bronze.events`** (ClickHouse HTTP insert).
3. **Airflow** DAG **`medallion_dbt_every_5m`** runs **`dbt build`** so silver/gold include new events.

**Batch**

1. CSVs live in **`data/batch/`** (`customers.csv`, `orders.csv`).
2. **Airflow** DAG **`medallion_batch_csv_to_gold`** truncates batch bronze tables (demo idempotency), loads CSVs into **`bronze.customers_raw`** / **`bronze.orders_raw`**, then runs **`dbt build`**.

### Orchestration vs ELT

- **Orchestration (ingest):** NiFi (realtime topology); Airflow (batch file load + schedules).
- **ELT transform:** dbt models read **bronze**, write **silver** and **gold**. ClickHouse is the primary analytics store.

### Services (Docker Compose)

- `clickhouse` — data platform.
- `postgres` — Airflow metadata.
- `airflow-webserver` / `airflow-scheduler` — DAGs and UI (`http://localhost:8080`, `admin` / `admin`).
- `nifi` — realtime ingestion/orchestration service (UI at `https://localhost:8443`).
- `event_sim` — continuous demo events (defaults to sending events to NiFi).

### Notes (honest MVP scope)

- Realtime ingestion is handled by **NiFi**. For troubleshooting only, the simulator can be configured to write directly to ClickHouse; this is not the intended architecture path.
