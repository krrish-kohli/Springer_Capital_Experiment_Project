{{ config(schema="silver") }}

SELECT
  toUInt64OrZero(order_id) AS order_id,
  toUInt64OrZero(customer_id) AS customer_id,
  toFloat64OrZero(order_total) AS order_total,
  parseDateTimeBestEffortOrNull(order_ts) AS order_ts,
  trimBoth(currency) AS currency
FROM {{ source("bronze", "orders_raw") }}
ORDER BY loaded_at DESC
LIMIT 1 BY toUInt64OrZero(order_id)
