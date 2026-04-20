{{ config(schema="silver") }}

SELECT
  toUInt64OrZero(customer_id) AS customer_id,
  trimBoth(email) AS email,
  upperUTF8(trimBoth(country_code)) AS country_code,
  parseDateTimeBestEffortOrNull(created_at) AS created_at,
  toDateOrNull(signup_date) AS signup_date
FROM {{ source("bronze", "customers_raw") }}
ORDER BY loaded_at DESC
LIMIT 1 BY toUInt64OrZero(customer_id)
