CREATE TABLE IF NOT EXISTS raw.customers (
  customer_id UInt64,
  email String,
  country_code String,
  created_at DateTime,
  signup_date Date
)
ENGINE = MergeTree
ORDER BY (customer_id);

CREATE TABLE IF NOT EXISTS raw.orders (
  order_id UInt64,
  customer_id UInt64,
  order_total Float64,
  order_ts DateTime,
  currency String
)
ENGINE = MergeTree
ORDER BY (order_id);

