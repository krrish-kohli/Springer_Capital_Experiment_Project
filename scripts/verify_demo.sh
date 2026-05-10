#!/usr/bin/env bash
# Smoke checks for the Medallion demo stack (host machine).
set -euo pipefail

CH_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
AIRFLOW_URL="${AIRFLOW_URL:-http://localhost:8080}"
NIFI_BASE="${NIFI_BASE:-https://localhost:8443}"

echo "== docker compose ps =="
docker compose -f "$(dirname "$0")/../docker-compose.yml" ps

echo ""
echo "== ClickHouse HTTP =="
curl -fsS "${CH_URL}/?query=SELECT%201%20FORMAT%20TabSeparated" | head -1

echo ""
echo "== Airflow webserver health =="
curl -fsS -o /dev/null -w "HTTP %{http_code}\n" "${AIRFLOW_URL}/health" || echo "(Airflow not reachable — is the stack up?)"

echo ""
echo "== NiFi API (JWT) =="
if command -v jq >/dev/null 2>&1; then
  tok=$(curl -sk -X POST "${NIFI_BASE%/}/nifi-api/access/token" \
    -d "username=${NIFI_USER:-admin}&password=${NIFI_PASS:-adminadminadmin}" \
    -H "Content-Type: application/x-www-form-urlencoded")
  if [[ ${#tok} -gt 100 ]]; then
    echo "NiFi token OK (${#tok} chars)"
    curl -sk -H "Authorization: Bearer ${tok}" "${NIFI_BASE%/}/nifi-api/process-groups/root/processors" |
      jq -r '.processors[]? | select(.component.name == "Medallion_ListenHTTP" or .component.name == "Medallion_InvokeHTTP_CH") | "\(.component.name): \(.status.aggregateSnapshot.runStatus // "unknown")"' ||
      echo "(Could not list Medallion processors — run ./scripts/bootstrap_nifi.sh)"
  else
    echo "NiFi token failed (check NIFI_BASE / credentials)"
  fi
else
  echo "jq not installed; skipping NiFi processor check"
fi

echo ""
echo "== ClickHouse row counts (bronze / silver / gold) =="
runq() {
  curl -fsS "${CH_URL}/" --data-binary "$1"
}

runq "SELECT 'bronze.events' AS tbl, toString(count()) AS n FROM bronze.events FORMAT TabSeparated" || echo "bronze.events: (query failed)"
runq "SELECT 'bronze.customers_raw' AS tbl, toString(count()) AS n FROM bronze.customers_raw FORMAT TabSeparated" || true
runq "SELECT 'bronze.orders_raw' AS tbl, toString(count()) AS n FROM bronze.orders_raw FORMAT TabSeparated" || true
runq "SELECT 'bronze.baserow_feedback_raw' AS tbl, toString(count()) AS n FROM bronze.baserow_feedback_raw FORMAT TabSeparated" || true
runq "SELECT 'silver.fct_events' AS tbl, toString(count()) AS n FROM silver.fct_events FORMAT TabSeparated" || echo "silver.fct_events: (missing or empty — run Airflow dbt DAGs)"
runq "SELECT 'silver.feedback_clean' AS tbl, toString(count()) AS n FROM silver.feedback_clean FORMAT TabSeparated" || true
runq "SELECT 'gold.mart_funnel_daily' AS tbl, toString(count()) AS n FROM gold.mart_funnel_daily FORMAT TabSeparated" || echo "gold.mart_funnel_daily: (missing or empty — run dbt)"
runq "SELECT 'gold.mart_sales_daily' AS tbl, toString(count()) AS n FROM gold.mart_sales_daily FORMAT TabSeparated" || true
runq "SELECT 'gold.mart_product_popularity' AS tbl, toString(count()) AS n FROM gold.mart_product_popularity FORMAT TabSeparated" || true
runq "SELECT 'gold.feedback_summary_daily' AS tbl, toString(count()) AS n FROM gold.feedback_summary_daily FORMAT TabSeparated" || true

echo ""
echo "Done. For detailed samples run: ./scripts/sample_queries.sh"
