#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DBT_DIR="$ROOT_DIR/dbt/silver_validation"

# Ensure dbt is available (local venv)
if [[ ! -x "$ROOT_DIR/.venv/bin/dbt" ]]; then
  echo "dbt not found in .venv. Bootstrapping python env..."
  "$ROOT_DIR/scripts/setup_venv.sh"
fi

# shellcheck disable=SC1091
source "$ROOT_DIR/.venv/bin/activate"

if [[ ! -f "$DBT_DIR/profiles.yml" ]]; then
  if [[ -f "$DBT_DIR/profiles.example.yml" ]]; then
    echo "profiles.yml not found. Using a temporary profiles.yml generated from profiles.example.yml."
  else
    echo "Missing dbt profiles. Create $DBT_DIR/profiles.yml" >&2
    exit 1
  fi
fi

RUN_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
RUN_TS="$(date '+%Y-%m-%d %H:%M:%S')"
CONFIG_HASH="$("$ROOT_DIR/scripts/config_hash.sh" "$ROOT_DIR/config/tables.yml")"

echo "Running dbt with:"
echo "  run_id=$RUN_ID"
echo "  run_ts=$RUN_TS"
echo "  config_hash=$CONFIG_HASH"

export CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
export CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
export CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
export CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
export CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-default}"

cd "$DBT_DIR"

TMP_PROFILES_DIR="$(mktemp -d)"
cp "$DBT_DIR/profiles.example.yml" "$TMP_PROFILES_DIR/profiles.yml"

dbt deps --profiles-dir "$TMP_PROFILES_DIR" || true

VARS="{\"run_id\":\"$RUN_ID\",\"run_ts\":\"$RUN_TS\",\"config_hash\":\"$CONFIG_HASH\"}"

echo
echo "Writing run start to ClickHouse (audit.audit_run_summary)..."
docker exec -i ch_silver_validation clickhouse-client --multiquery <<SQL >/dev/null
INSERT INTO audit.audit_run_summary
(run_id, run_ts, run_end_ts, status, trigger_type, scope, dbt_job_id, dbt_invocation_id, total_rules, failed_rules, warn_rules, exception_rows, error_message, config_hash)
VALUES
(toUUID('$RUN_ID'), toDateTime('$RUN_TS'), NULL, 'running', 'manual', '{"tables":["customers","orders"]}', NULL, NULL, 0, 0, 0, 0, NULL, '$CONFIG_HASH');
SQL

echo
echo "Step 1/2: materialize validation outputs (runs even if tests fail)"
dbt run \
  --profiles-dir "$TMP_PROFILES_DIR" \
  --select "staging+ validation+ marts+" \
  --vars "$VARS"

echo
echo "Step 2/2: execute dbt tests (non-zero exit indicates validation failures)"
set +e
dbt test --profiles-dir "$TMP_PROFILES_DIR" --vars "$VARS"
TEST_EXIT=$?
set -e

echo
echo "Finalizing run summary in ClickHouse..."
docker exec -i ch_silver_validation clickhouse-client --multiquery <<SQL >/dev/null
ALTER TABLE audit.audit_run_summary
UPDATE
  run_end_ts = now(),
  status = if($TEST_EXIT = 0, 'success', 'failed'),
  total_rules = (SELECT toUInt32(count()) FROM audit.audit_results WHERE run_id = toUUID('$RUN_ID')),
  failed_rules = (SELECT toUInt32(countIf(status = 'fail' OR status = 'error')) FROM audit.audit_results WHERE run_id = toUUID('$RUN_ID')),
  warn_rules = (SELECT toUInt32(countIf(status = 'warn')) FROM audit.audit_results WHERE run_id = toUUID('$RUN_ID')),
  exception_rows = (SELECT toUInt64(count()) FROM audit.audit_exceptions WHERE run_id = toUUID('$RUN_ID')),
  error_message = if($TEST_EXIT = 0, NULL, 'dbt tests failed (see dbt output)')
WHERE run_id = toUUID('$RUN_ID');
SQL

exit "$TEST_EXIT"

