# ClickHouse DDL

Scripts in [`ddl/`](ddl/) run on first container start via `/docker-entrypoint-initdb.d`.

- `001_databases.sql` — `bronze`, `silver`, `gold`
- `010_bronze_tables.sql` — landing tables for events and batch CSV loads

**Silver** and **gold** physical tables are created by **dbt** (`dbt/medallion_demo`).

Optional sample rows: [`seed/001_seed_demo_data.sql`](seed/001_seed_demo_data.sql) (see `scripts/seed_demo_data.sh`).
