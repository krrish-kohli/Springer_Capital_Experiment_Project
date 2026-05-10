#!/usr/bin/env bash
set -euo pipefail

CH_URL="${CLICKHOUSE_URL:-http://localhost:8123}"

safe_query() {
  local title=$1
  local sql=$2
  echo "$title"
  if ! curl -fsS "${CH_URL}/" --data-binary "$sql"; then
    echo "(query failed — run NiFi bootstrap + Airflow dbt DAGs if tables are missing)"
  fi
  echo ""
}

safe_query "bronze.events (latest 10)" \
  "SELECT event_ts, event_name, user_id, properties FROM bronze.events ORDER BY ingest_ts DESC LIMIT 10 FORMAT PrettyCompact"

safe_query "bronze.events (row count)" \
  "SELECT count() AS bronze_events FROM bronze.events FORMAT PrettyCompact"

safe_query "silver.fct_events (latest 10)" \
  "SELECT event_ts, event_name, product_id, revenue FROM silver.fct_events ORDER BY ingest_ts DESC LIMIT 10 FORMAT PrettyCompact"

safe_query "silver.fct_events (row count)" \
  "SELECT count() AS silver_events FROM silver.fct_events FORMAT PrettyCompact"

safe_query "gold.mart_funnel_daily" \
  "SELECT * FROM gold.mart_funnel_daily ORDER BY event_date DESC LIMIT 20 FORMAT PrettyCompact"

safe_query "gold.mart_sales_daily" \
  "SELECT * FROM gold.mart_sales_daily ORDER BY order_date DESC LIMIT 20 FORMAT PrettyCompact"

safe_query "gold.mart_product_popularity" \
  "SELECT * FROM gold.mart_product_popularity LIMIT 20 FORMAT PrettyCompact"

safe_query "bronze.baserow_feedback_raw (latest 10)" \
  "SELECT feedback_id, feedback_date, rating, product_name, region FROM bronze.baserow_feedback_raw ORDER BY ingest_ts DESC LIMIT 10 FORMAT PrettyCompact"

safe_query "silver.feedback_clean (latest 10)" \
  "SELECT feedback_id, feedback_date, rating, product_name FROM silver.feedback_clean ORDER BY ingest_ts DESC LIMIT 10 FORMAT PrettyCompact"

safe_query "gold.feedback_summary_daily" \
  "SELECT * FROM gold.feedback_summary_daily ORDER BY feedback_date DESC LIMIT 20 FORMAT PrettyCompact"

safe_query "gold.avg_rating_by_product" \
  "SELECT * FROM gold.avg_rating_by_product LIMIT 20 FORMAT PrettyCompact"

safe_query "gold.feedback_by_region" \
  "SELECT * FROM gold.feedback_by_region LIMIT 20 FORMAT PrettyCompact"
