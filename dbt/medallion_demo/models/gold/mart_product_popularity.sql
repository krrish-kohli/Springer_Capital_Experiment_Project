{{ config(schema="gold") }}

SELECT
  coalesce(nullIf(product_id, ''), 'unknown') AS product_id,
  countIf(event_name = 'product_view') AS product_views,
  countIf(event_name = 'purchase') AS purchases
FROM {{ ref("fct_events") }}
GROUP BY 1
ORDER BY product_views DESC
