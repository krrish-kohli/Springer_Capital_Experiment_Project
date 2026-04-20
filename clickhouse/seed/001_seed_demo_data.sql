-- Optional: sample bronze events for local checks (no NiFi). Airflow/dbt demos use CSV + pipelines.
INSERT INTO bronze.events (event_id, event_ts, event_name, user_id, session_id, properties, ingest_ts, ingest_source, _raw)
VALUES
  (generateUUIDv4(), now64(3), 'page_view', 'u1', 's1', '{"page":"/"}', now64(3), 'seed', NULL),
  (generateUUIDv4(), now64(3), 'product_view', 'u1', 's1', '{"product_id":"SKU-1"}', now64(3), 'seed', NULL);
