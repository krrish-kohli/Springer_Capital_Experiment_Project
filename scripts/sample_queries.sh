#!/usr/bin/env bash
set -euo pipefail

echo "Latest run summary rows:"
Q1="$(python3 -c "import urllib.parse; q=\"SELECT run_id, run_ts, status, failed_rules, exception_rows, error_message FROM audit.audit_run_summary ORDER BY run_ts DESC LIMIT 5\"; print(urllib.parse.quote(q))")"
curl -fsS "http://localhost:8123/?query=$Q1" | sed 's/\\\\N/NULL/g'

echo
echo "Latest failing tables:"
Q2="$(python3 -c "import urllib.parse; q=\"SELECT * FROM marts.failing_tables_latest LIMIT 50\"; print(urllib.parse.quote(q))")"
curl -fsS "http://localhost:8123/?query=$Q2" | sed 's/\\\\N/NULL/g'

echo
echo "Latest audit results (rule-level):"
Q3="$(python3 -c "import urllib.parse; q=\"SELECT run_ts, domain, table_name, rule_name, status, observed_value, observed_count, details FROM audit.audit_results ORDER BY run_ts DESC, domain, table_name, rule_name LIMIT 50\"; print(urllib.parse.quote(q))")"
curl -fsS "http://localhost:8123/?query=$Q3" | sed 's/\\\\N/NULL/g'

