{{ config(schema="gold") }}

SELECT
  toDate(order_ts) AS order_date,
  currency,
  count() AS order_count,
  sum(order_total) AS revenue
FROM {{ ref("fct_orders") }}
GROUP BY 1, 2
ORDER BY 1, 2
