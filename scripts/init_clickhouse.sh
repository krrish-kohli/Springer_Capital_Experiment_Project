#!/usr/bin/env bash
set -euo pipefail

echo "Creating databases + tables via ClickHouse container init scripts..."
echo "If you started ClickHouse with docker compose, DDL in clickhouse/ddl is auto-applied."

echo "Verifying ClickHouse is reachable..."
curl -fsS "http://localhost:8123/?query=SELECT%201" >/dev/null
echo "OK"

echo "Applying DDL explicitly (idempotent)..."
for f in clickhouse/ddl/*.sql; do
  echo " - $f"
  docker exec -i ch_silver_validation clickhouse-client --multiquery < "$f" >/dev/null
done

echo "Done."

