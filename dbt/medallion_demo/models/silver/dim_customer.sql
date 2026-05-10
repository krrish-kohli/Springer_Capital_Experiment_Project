{{ config(schema="silver") }}

SELECT
  toUInt64OrZero(toString(customer_id)) AS customer_id,
  trimBoth(toString(email)) AS email,
  upperUTF8(trimBoth(toString(country_code))) AS country_code,
  parseDateTimeBestEffortOrNull(toString(created_at)) AS created_at,
  toDateOrNull(toString(signup_date)) AS signup_date
FROM {{ source("bronze", "customers_raw") }}
ORDER BY loaded_at DESC
LIMIT 1 BY toUInt64OrZero(toString(customer_id))
