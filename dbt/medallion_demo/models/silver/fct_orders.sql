{{ config(schema="silver") }}

SELECT
  toUInt64OrZero(toString(order_id)) AS order_id,
  toUInt64OrZero(toString(customer_id)) AS customer_id,
  toFloat64OrZero(toString(order_total)) AS order_total,
  parseDateTimeBestEffortOrNull(toString(order_ts)) AS order_ts,
  trimBoth(toString(currency)) AS currency
FROM {{ source("bronze", "orders_raw") }}
ORDER BY loaded_at DESC
LIMIT 1 BY toUInt64OrZero(toString(order_id))
