# Medallion demo — NiFi (realtime) + Airflow + dbt → ClickHouse

Internship-friendly **dual pipeline** into one ClickHouse warehouse:

| Path | Ingest | Transform / orchestration |
|------|--------|---------------------------|
| **Realtime** | `event_sim` → **NiFi** (ListenHTTP) → **ClickHouse** `bronze.events` | **Airflow** runs `dbt build` on a short schedule |
| **Batch** | **Airflow** loads `data/batch/*.csv` → `bronze.*_raw` | Same **dbt** project → `silver` / `gold` |

## Quickstart

1. **Start the stack**

On macOS/Linux, match your user id so Airflow can write `./airflow/logs`:

```bash
export AIRFLOW_UID="$(id -u)"
docker compose up -d --build
```

2. **Airflow UI** — [http://localhost:8080](http://localhost:8080) (`admin` / `admin`).

3. **Trigger batch DAG** — open DAG **`medallion_batch_csv_to_gold`** → **Trigger DAG**. This loads CSVs into bronze and runs `dbt build`.

4. **Realtime** — `event_sim` sends events to **NiFi** by default (`EVENT_TARGET=nifi` in `docker-compose.yml`). NiFi lands events in `bronze.events`.

5. **Transform refresh (realtime)** — DAG **`medallion_dbt_every_5m`** runs `dbt build` every five minutes so `silver`/`gold` reflect new events.

6. **Query**

```bash
chmod +x scripts/sample_queries.sh
./scripts/sample_queries.sh
```

**ClickHouse HTTP:** [http://localhost:8123](http://localhost:8123)  
**NiFi UI (HTTPS):** [https://localhost:8443](https://localhost:8443) (`admin` / `adminadminadmin`)

## Sanity checks (proof of bronze → silver → gold)

Run:

```bash
./scripts/sample_queries.sh
```

You should see rows in:
- `bronze.events` (realtime landing, written by NiFi)
- `silver.fct_events` (dbt silver)
- `gold.*` marts (dbt gold)

## Troubleshooting (advanced)

See [docs/troubleshooting.md](docs/troubleshooting.md) for a troubleshooting-only shortcut that bypasses NiFi.

## Recommended demo flow (10 minutes)

1. **Architecture (60s)**: bronze/silver/gold in ClickHouse; **NiFi** is realtime ingestion; **Airflow + dbt** do batch + scheduled transforms.
2. **Realtime proof**:
   - NiFi UI: flow running
   - ClickHouse: `bronze.events` count increasing
   - Airflow: `medallion_dbt_every_5m` succeeded
   - ClickHouse: `silver.fct_events` and `gold.*` populated
3. **Batch proof**:
   - Airflow: trigger `medallion_batch_csv_to_gold`
   - ClickHouse: `bronze.customers_raw` / `bronze.orders_raw` loaded
   - ClickHouse: `silver.dim_customer`, `silver.fct_orders`, `gold.mart_sales_daily` updated

## Repo layout

| Path | Purpose |
|------|---------|
| [clickhouse/ddl/](clickhouse/ddl/) | Bronze databases/tables |
| [dbt/medallion_demo/](dbt/medallion_demo/) | Silver + gold models |
| [airflow/dags/](airflow/dags/) | Batch load + scheduled dbt |
| [data/batch/](data/batch/) | `customers.csv`, `orders.csv` |
| [event_sim/](event_sim/) | Synthetic `page_view` / `purchase` events |
| [nifi/flow/](nifi/flow/) | NiFi ingest template |

## Docs

- [docs/architecture.md](docs/architecture.md)
- [docs/nifi_flow.md](docs/nifi_flow.md)

## Local ClickHouse DDL (optional)

If you need to re-apply DDL against a running container:

```bash
./scripts/init_clickhouse.sh
```

Optional bronze event samples:

```bash
./scripts/seed_demo_data.sh
```

## dbt locally

See [dbt/README.md](dbt/README.md). Copy `dbt/medallion_demo/profiles.example.yml` to `~/.dbt/profiles.yml` and run `dbt build` from `dbt/medallion_demo`.
