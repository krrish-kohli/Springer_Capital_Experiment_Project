-- Bronze: append-only landing layer (Medallion). Silver/gold are built by dbt.

CREATE TABLE IF NOT EXISTS bronze.events (
  event_id UUID,
  event_ts DateTime64(3),
  event_name LowCardinality(String),
  user_id Nullable(String),
  session_id Nullable(String),
  properties String,
  ingest_ts DateTime64(3),
  ingest_source LowCardinality(String),
  _raw Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(event_ts))
ORDER BY (event_ts, event_name, event_id);

CREATE TABLE IF NOT EXISTS bronze.customers_raw (
  load_id UUID,
  file_name LowCardinality(String),
  loaded_at DateTime64(3),
  customer_id String,
  email String,
  country_code String,
  created_at String,
  signup_date String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDate(loaded_at))
ORDER BY (load_id, customer_id);

CREATE TABLE IF NOT EXISTS bronze.orders_raw (
  load_id UUID,
  file_name LowCardinality(String),
  loaded_at DateTime64(3),
  order_id String,
  customer_id String,
  order_total String,
  order_ts String,
  currency String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDate(loaded_at))
ORDER BY (load_id, order_id);
