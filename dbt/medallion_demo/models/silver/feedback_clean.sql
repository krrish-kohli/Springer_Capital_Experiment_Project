{{ config(schema="silver") }}

SELECT
  toUInt64(id) AS baserow_row_id,
  trimBoth(feedback_id) AS feedback_id,
  feedback_date,
  nullIf(trimBoth(feedback_label), '') AS feedback_label,
  toFloat32OrNull(nullIf(trimBoth(toString(rating)), '')) AS rating,
  nullIf(trimBoth(feedback_text), '') AS feedback_text,
  nullIf(trimBoth(customer_id), '') AS customer_id,
  nullIf(trimBoth(customer_name), '') AS customer_name,
  nullIf(trimBoth(product_name), '') AS product_name,
  nullIf(trimBoth(region), '') AS region,
  ingest_ts
FROM {{ source("bronze", "baserow_feedback_raw") }}
WHERE feedback_date > toDate('1970-01-02')
