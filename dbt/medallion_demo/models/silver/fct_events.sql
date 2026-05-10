{{ config(schema="silver") }}

SELECT
  event_id,
  tupleElement(t, 1) AS event_ts,
  tupleElement(t, 2) AS event_name,
  tupleElement(t, 3) AS user_id,
  tupleElement(t, 4) AS session_id,
  tupleElement(t, 5) AS product_id,
  tupleElement(t, 6) AS page_path,
  tupleElement(t, 7) AS revenue,
  tupleElement(t, 8) AS properties,
  tupleElement(t, 9) AS ingest_ts
FROM (
  SELECT
    event_id,
    argMax(
      tuple(
        event_ts,
        event_name,
        user_id,
        session_id,
        JSONExtractString(properties, 'product_id'),
        JSONExtractString(properties, 'page'),
        toFloat64OrZero(JSONExtractString(properties, 'revenue')),
        properties,
        ingest_ts
      ),
      ingest_ts
    ) AS t
  FROM {{ source("bronze", "events") }}
  GROUP BY event_id
)
