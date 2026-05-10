# Medallion demo — NiFi (realtime) + Airflow + dbt → ClickHouse

Internship-friendly **multi-source** pipelines into one ClickHouse warehouse:

| Path | Ingest | Transform / orchestration |
|------|--------|---------------------------|
| **Realtime** | `event_sim` → **NiFi** (ListenHTTP) → **ClickHouse** `bronze.events` | **Airflow** runs `dbt build` on a short schedule |
| **Batch (CSV)** | **Airflow** loads `data/batch/*.csv` → `bronze.*_raw` | Same **dbt** project → `silver` / `gold` |
| **Batch (Baserow)** | **Airflow** calls **Baserow REST API** → `bronze.baserow_feedback_raw` | DAG runs **`dbt build`** scoped to the Baserow lineage |

## Quickstart

1. **Secrets for Baserow (optional but needed for that DAG)** — copy [`.env.example`](.env.example) to **`.env`** and set `BASEROW_API_TOKEN`. Docker Compose reads `.env` for variable substitution into Airflow services.

2. **Start the stack**

```bash
docker compose up -d --build
```

3. **Provision NiFi (realtime ingest)** — creates **Medallion_ListenHTTP** → **Medallion_InvokeHTTP_CH** → ClickHouse `bronze.events` (requires `curl`, `jq`, `python3` on the host):

```bash
chmod +x scripts/bootstrap_nifi.sh
./scripts/bootstrap_nifi.sh
```

4. **Airflow UI** — [http://localhost:8080](http://localhost:8080) (`admin` / `admin`). On Linux, if Airflow cannot write logs, run `export AIRFLOW_UID="$(id -u)"` before step 2.

5. **Trigger DAGs** as needed:
   - **`medallion_batch_csv_to_gold`** — CSVs into bronze + full `dbt build`
   - **`baserow_feedback_to_gold`** — fetch Baserow rows → bronze → `dbt build` for Baserow models only

6. **Realtime** — with step 3 done, `event_sim` posts JSON to NiFi (`EVENT_TARGET=nifi` in `docker-compose.yml`); NiFi inserts into `bronze.events`.

7. **Transform refresh (realtime + global)** — DAG **`medallion_dbt_every_5m`** runs `dbt build` every five minutes (wait for a run or trigger once).

8. **Verify**

```bash
chmod +x scripts/verify_demo.sh scripts/sample_queries.sh
./scripts/verify_demo.sh
./scripts/sample_queries.sh
```

**ClickHouse HTTP:** [http://localhost:8123](http://localhost:8123)  
**NiFi UI (HTTPS):** [https://localhost:8443](https://localhost:8443) (`admin` / `adminadminadmin`)

## Sanity checks (proof of bronze → silver → gold)

Run:

```bash
./scripts/verify_demo.sh
./scripts/sample_queries.sh
```

You should see rows in:
- `bronze.events` (realtime, NiFi) and **`bronze.baserow_feedback_raw`** after running the Baserow DAG
- `silver.fct_events`, **`silver.feedback_clean`** (when loaded)
- **`gold.*`** marts (CSV-, event-, and Baserow-derived)

## Troubleshooting (advanced)

See [docs/troubleshooting.md](docs/troubleshooting.md) for a troubleshooting-only shortcut that bypasses NiFi.

## Recommended demo flow (10 minutes)

1. **Architecture (60s)**: bronze/silver/gold in ClickHouse; **NiFi** is realtime ingestion; **Airflow + dbt** orchestrate CSV batch, **Baserow API batch**, and scheduled transforms.
2. **Realtime proof**:
   - `./scripts/bootstrap_nifi.sh` done; NiFi UI: **Medallion_ListenHTTP** / **Medallion_InvokeHTTP_CH** running
   - ClickHouse: `bronze.events` count increasing
   - Airflow: `medallion_dbt_every_5m` succeeded (or trigger once)
   - ClickHouse: `silver.fct_events` and event gold marts populated
3. **Batch CSV proof**:
   - Airflow: trigger `medallion_batch_csv_to_gold`
   - ClickHouse: `bronze.customers_raw` / `bronze.orders_raw` loaded
   - ClickHouse: `silver.dim_customer`, `silver.fct_orders`, `gold.mart_sales_daily` updated
4. **Baserow proof**:
   - `.env` contains `BASEROW_API_TOKEN`; trigger **`baserow_feedback_to_gold`**
   - ClickHouse: `bronze.baserow_feedback_raw`, `silver.feedback_clean`, `gold.feedback_summary_daily` / product / region marts

## Repo layout

| Path | Purpose |
|------|---------|
| [clickhouse/ddl/](clickhouse/ddl/) | Bronze databases/tables |
| [dbt/medallion_demo/](dbt/medallion_demo/) | Silver + gold models |
| [airflow/dags/](airflow/dags/) | Batch load, Baserow ingest, scheduled dbt |
| [data/batch/](data/batch/) | `customers.csv`, `orders.csv` |
| [event_sim/](event_sim/) | Synthetic `page_view` / `purchase` events |
| [nifi/flow/](nifi/flow/) | NiFi flow documentation |
| [scripts/bootstrap_nifi.sh](scripts/bootstrap_nifi.sh) | Auto-provision NiFi ListenHTTP → ClickHouse |
| [scripts/verify_demo.sh](scripts/verify_demo.sh) | Stack smoke checks |
| [`.env.example`](.env.example) | Template for `BASEROW_*` secrets (copy to `.env`) |

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

## About This Project
Data pipeline experiment using Medallion architecture.
