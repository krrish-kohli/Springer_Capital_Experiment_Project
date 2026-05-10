{{ config(schema="gold") }}

SELECT
  coalesce(product_name, 'unknown') AS product_name,
  avg(rating) AS avg_rating,
  count() AS feedback_count
FROM {{ ref("feedback_clean") }}
GROUP BY 1
ORDER BY avg_rating DESC NULLS LAST
