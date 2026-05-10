{{ config(schema="gold") }}

SELECT
  toDate(feedback_date) AS feedback_date,
  count() AS feedback_count,
  avg(rating) AS avg_rating
FROM {{ ref("feedback_clean") }}
GROUP BY 1
ORDER BY 1
