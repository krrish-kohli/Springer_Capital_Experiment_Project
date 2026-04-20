#!/usr/bin/env bash
set -euo pipefail

CH_URL="${CLICKHOUSE_URL:-http://localhost:8123}"

echo "bronze.events (latest 10)"
curl -fsS "${CH_URL}/" --data-binary "SELECT event_ts, event_name, user_id, properties FROM bronze.events ORDER BY ingest_ts DESC LIMIT 10 FORMAT PrettyCompact"

echo ""
echo "bronze.events (row count)"
curl -fsS "${CH_URL}/" --data-binary "SELECT count() AS bronze_events FROM bronze.events FORMAT PrettyCompact"

echo ""
echo "silver.fct_events (latest 10)"
curl -fsS "${CH_URL}/" --data-binary "SELECT event_ts, event_name, product_id, revenue FROM silver.fct_events ORDER BY ingest_ts DESC LIMIT 10 FORMAT PrettyCompact"

echo ""
echo "silver.fct_events (row count)"
curl -fsS "${CH_URL}/" --data-binary "SELECT count() AS silver_events FROM silver.fct_events FORMAT PrettyCompact"

echo ""
echo "gold.mart_funnel_daily"
curl -fsS "${CH_URL}/" --data-binary "SELECT * FROM gold.mart_funnel_daily ORDER BY event_date DESC LIMIT 20 FORMAT PrettyCompact"

echo ""
echo "gold.mart_sales_daily"
curl -fsS "${CH_URL}/" --data-binary "SELECT * FROM gold.mart_sales_daily ORDER BY order_date DESC LIMIT 20 FORMAT PrettyCompact"

echo ""
echo "gold.mart_product_popularity"
curl -fsS "${CH_URL}/" --data-binary "SELECT * FROM gold.mart_product_popularity LIMIT 20 FORMAT PrettyCompact"
