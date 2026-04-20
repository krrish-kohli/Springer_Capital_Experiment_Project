#!/usr/bin/env bash
set -euo pipefail

echo "Optional seed: sample bronze.events rows (idempotent-friendly truncate not applied; safe to re-run seed SQL)."

docker exec -i ch_medallion clickhouse-client --multiquery < clickhouse/seed/001_seed_demo_data.sql >/dev/null

echo "Done."
