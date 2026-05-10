{{ config(schema="gold") }}

SELECT
  coalesce(region, 'unknown') AS region,
  count() AS feedback_count,
  avg(rating) AS avg_rating
FROM {{ ref("feedback_clean") }}
GROUP BY 1
ORDER BY feedback_count DESC
