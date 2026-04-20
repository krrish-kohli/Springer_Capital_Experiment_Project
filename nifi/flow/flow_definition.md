## NiFi flow — realtime events → ClickHouse bronze

NiFi **ingests** simulated (or real) JSON events and lands them in `bronze.events`. **dbt** (scheduled via Airflow) promotes data to `silver` / `gold`; NiFi does not run dbt.

### Preconditions

- Stack running via Docker Compose: `clickhouse`, `nifi` (ports `8443` UI, `8081` ingest).
- Table `bronze.events` exists (from `clickhouse/ddl/010_bronze_tables.sql`).

### Parameter context (recommended)

Create a Parameter Context `medallion_demo` with:

- `CH_HTTP_URL`: `http://clickhouse:8123`
- `CH_INSERT_QUERY`: `INSERT INTO bronze.events (event_id, event_ts, event_name, user_id, session_id, properties, ingest_ts, ingest_source, _raw) FORMAT JSONEachRow`

### Processor chain (template)

1. **ListenHTTP**
   - **Listening Port**: `8081` (matches `docker-compose` port mapping `8081:8081`).
   - **Base Path**: `/contentListener` (matches `event_sim` default `NIFI_INGEST_URL`).

2. **UpdateAttribute** (optional enrichment)
   - `ingest_ts`: `${now():format("yyyy-MM-dd HH:mm:ss")}`
   - `ingest_source`: `nifi_http`

3. **JoltTransformJSON** or **ReplaceText** (optional)
   - Ensure the FlowFile body is one **JSON object** matching ClickHouse columns, e.g. merge `ingest_ts` / `ingest_source` into the payload if the producer did not send them.

4. **InvokeHTTP** (ClickHouse HTTP insert)
   - **HTTP Method**: `POST`
   - **Remote URL**: `#{CH_HTTP_URL}/?query=#{CH_INSERT_QUERY}` (URL-encode the query in the parameter or use EL carefully).
   - **Send Message Body**: `true`
   - **Content-Type**: `application/json` (body is a single JSONEachRow line).

5. **RouteOnAttribute**
   - Success when `invokehttp.status.code` equals `200`.

6. **LogAttribute** / **PutFile** (failure path)
   - Capture failures for demo troubleshooting.

### Expected JSON body (per event)

Fields should align with `bronze.events`:

- `event_id` (UUID string)
- `event_ts` (`YYYY-MM-DD HH:MM:SS` or ISO)
- `event_name` (`page_view`, `product_view`, `add_to_cart`, `purchase`)
- `user_id`, `session_id` (strings)
- `properties` (JSON **string** containing keys like `page`, `product_id`, `revenue`)
- `ingest_ts`, `ingest_source` (can be added in NiFi)
- `_raw` (optional)

### Event simulator → NiFi

By default (per `docker-compose.yml`), `event_sim` sends events to NiFi (`EVENT_TARGET=nifi`). Ensure this flow is active.\n+\n+If you are troubleshooting ClickHouse connectivity and need to bypass NiFi temporarily, set `EVENT_TARGET=clickhouse`. This shortcut is documented in [docs/troubleshooting.md](../../docs/troubleshooting.md) and is **not** the intended architecture.

### Operational notes

- Use **backpressure** on failure routes so bad messages do not overwhelm ClickHouse.
- For production patterns you would add **SSL**, **auth**, and **dead-letter** queues; this repo stays minimal for demos.
