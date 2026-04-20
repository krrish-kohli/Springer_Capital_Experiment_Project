{{ config(schema="gold") }}

SELECT
  toDate(event_ts) AS event_date,
  event_name,
  uniqExact(session_id) AS sessions,
  uniqExact(user_id) AS users,
  count() AS event_count
FROM {{ ref("fct_events") }}
GROUP BY 1, 2
ORDER BY 1, 2
