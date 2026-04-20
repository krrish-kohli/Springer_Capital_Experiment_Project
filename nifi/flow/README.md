## NiFi flow export

- `flow_definition.md` — step-by-step processors to land JSON events into `bronze.events`.
- `flow_definition.json` — short machine-readable summary.

NiFi provides **realtime ingestion** (ListenHTTP, routing, backpressure). **Airflow + dbt** handle batch CSV loads and all **silver/gold** transformations.
