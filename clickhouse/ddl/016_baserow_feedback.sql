-- Baserow API landing (Airflow → bronze). rating kept as String to match API JSON types.

CREATE TABLE IF NOT EXISTS bronze.baserow_feedback_raw
(
    id UInt64,
    feedback_label String,
    feedback_date Date,
    rating String,
    feedback_text String,
    feedback_id String,
    customer_id String,
    customer_name String,
    product_name String,
    region String,
    ingest_ts DateTime64(3),
    _raw String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(feedback_date)
ORDER BY (feedback_date, feedback_id);
