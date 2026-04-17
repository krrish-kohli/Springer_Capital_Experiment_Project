#!/usr/bin/env bash
set -euo pipefail

echo "Seeding demo raw/silver data..."

# Reset demo tables so seeding is idempotent.
docker exec -i ch_silver_validation clickhouse-client --multiquery <<'SQL' >/dev/null
TRUNCATE TABLE IF EXISTS raw.customers;
TRUNCATE TABLE IF EXISTS silver.customers;
TRUNCATE TABLE IF EXISTS raw.orders;
TRUNCATE TABLE IF EXISTS silver.orders;
SQL

docker exec -i ch_silver_validation clickhouse-client --multiquery < clickhouse/seed/001_seed_demo_data.sql >/dev/null

echo "Done."

