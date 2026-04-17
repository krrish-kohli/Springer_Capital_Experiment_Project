## NiFi flow (reproducible definition)

This MVP uses NiFi as an orchestration/control plane that triggers the `runner` service.

### Preconditions
- Stack running via Docker Compose:
  - ClickHouse: `clickhouse`
  - Runner: `runner` (HTTP on `http://runner:8080`)
- NiFi UI available on `https://localhost:8443` (default credentials from `docker-compose.yml`)

### Parameter Context (recommended)
Create a Parameter Context named `silver_validation` with:
- `RUNNER_URL`: `http://runner:8080/runs`
- `DEFAULT_SCOPE_JSON`: `{"domains":["customer","sales"],"tables":["customers","orders"]}`

### Flow steps (processors)
1. **GenerateFlowFile**
   - Schedule: e.g. every 5 minutes (or disable and use manual run)

2. **GenerateUUID**
   - UUID Attribute Name: `run_id`

3. **UpdateAttribute**
   - Add attribute: `run_ts`
   - Value: `${now():format("yyyy-MM-dd HH:mm:ss")}`
   - Add attribute: `scope_json`
   - Value: `#{DEFAULT_SCOPE_JSON}`

4. **ReplaceText** (create JSON body)
   - Replacement Strategy: `Always Replace`
   - Replacement Value:
     ```json
     {
       "run_id": "${run_id}",
       "run_ts": "${run_ts}",
       "scope": ${scope_json}
     }
     ```

5. **InvokeHTTP**
   - HTTP Method: `POST`
   - Remote URL: `#{RUNNER_URL}`
   - Send Message Body: `true`
   - Content-Type: `application/json`
   - Read Response Body: `true`

6. **RouteOnAttribute**
   - Route: `success` when `${invokehttp.status.code:equals("200")}`
   - Route: `failure` otherwise

7. (Optional) **LogMessage**
   - Log Level: `info`
   - Message: `Validation run ${run_id} returned ${invokehttp.status.code}`

### Expected behavior
- On trigger, NiFi generates `run_id`/`run_ts`, calls the runner, and routes based on HTTP response code.
- ClickHouse tables are updated by the runner/dbt:
  - `audit.audit_run_summary`
  - `audit.audit_results`
  - `audit.audit_exceptions`
