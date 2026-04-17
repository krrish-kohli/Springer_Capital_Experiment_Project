CREATE TABLE IF NOT EXISTS audit.audit_run_summary (
  run_id UUID,
  run_ts DateTime,
  run_end_ts Nullable(DateTime),
  status Enum8('running' = 1, 'success' = 2, 'failed' = 3, 'error' = 4),
  trigger_type LowCardinality(String),
  scope String,
  dbt_job_id Nullable(String),
  dbt_invocation_id Nullable(String),
  total_rules UInt32,
  failed_rules UInt32,
  warn_rules UInt32,
  exception_rows UInt64,
  error_message Nullable(String),
  config_hash FixedString(64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(run_ts)
ORDER BY (run_ts, run_id);

CREATE TABLE IF NOT EXISTS audit.audit_results (
  run_id UUID,
  run_ts DateTime,
  domain LowCardinality(String),
  table_name String,
  silver_table String,
  rule_name LowCardinality(String),
  status Enum8('pass' = 1, 'warn' = 2, 'fail' = 3, 'error' = 4),
  observed_value Nullable(Float64),
  observed_count Nullable(UInt64),
  threshold Nullable(Float64),
  expected_min Nullable(Float64),
  expected_max Nullable(Float64),
  details String,
  config_hash FixedString(64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(run_ts)
ORDER BY (domain, table_name, run_ts, rule_name, run_id);

CREATE TABLE IF NOT EXISTS audit.audit_exceptions (
  run_id UUID,
  run_ts DateTime,
  domain LowCardinality(String),
  table_name String,
  silver_table String,
  rule_name LowCardinality(String),
  pk String,
  exception_type LowCardinality(String),
  column_name Nullable(String),
  bad_value Nullable(String),
  row_snapshot String,
  config_hash FixedString(64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(run_ts)
ORDER BY (domain, table_name, run_ts, rule_name, run_id);

