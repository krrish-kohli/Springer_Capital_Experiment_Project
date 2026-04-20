{{ config(schema="silver") }}

SELECT
  event_id,
  argMax(event_ts, ingest_ts) AS event_ts,
  argMax(event_name, ingest_ts) AS event_name,
  argMax(user_id, ingest_ts) AS user_id,
  argMax(session_id, ingest_ts) AS session_id,
  argMax(JSONExtractString(properties, 'product_id'), ingest_ts) AS product_id,
  argMax(JSONExtractString(properties, 'page'), ingest_ts) AS page_path,
  argMax(toFloat64OrZero(JSONExtractString(properties, 'revenue')), ingest_ts) AS revenue,
  argMax(properties, ingest_ts) AS properties,
  max(ingest_ts) AS ingest_ts
FROM {{ source("bronze", "events") }}
GROUP BY event_id
