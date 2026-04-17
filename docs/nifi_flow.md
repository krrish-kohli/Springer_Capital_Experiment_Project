## NiFi flow (MVP) – Orchestrate validation via Runner

NiFi is the **control plane**. It does not run dbt itself; it **calls the Runner service** which executes dbt and updates ClickHouse audit tables.

### NiFi’s responsibilities
- Generate run metadata (`run_id`, `run_ts`)
- (Optionally) set scope (domains/tables) for the run
- Call the Runner (`POST /runs`)
- Route success vs failure
- (Optional) send alerts (Slack/email)

### Runner endpoint contract
**Endpoint**: `POST http://runner:8080/runs`

**Request JSON (MVP)**
```json
{
  "run_id": "uuid-string",
  "run_ts": "YYYY-MM-DD HH:MM:SS",
  "scope": {
    "domains": ["customer", "sales"],
    "tables": ["customers", "orders"]
  }
}
```

**Response JSON**
```json
{
  "run_id": "uuid-string",
  "status": "success|failed|error",
  "failed_rules": 7,
  "warn_rules": 0,
  "exception_rows": 30
}
```

### High-level processor chain (practical)
1. **GenerateFlowFile** (schedule) or **HandleHttpRequest** (manual trigger)
2. **GenerateUUID** → attribute `run_id`
3. **UpdateAttribute** → `run_ts=${now():format("yyyy-MM-dd HH:mm:ss")}`
4. **AttributesToJSON** (or `ReplaceText`) → build request body JSON
5. **InvokeHTTP**
   - URL: `http://runner:8080/runs`
   - Method: `POST`
   - Send message body: true
   - Content-Type: `application/json`
6. **RouteOnAttribute**
   - route on `invokehttp.status.code` (e.g. 200 vs non-200)
7. Optional:
   - **LogMessage** (write a concise run summary)
   - **PutEmail** / Slack processor (alert on non-200 or `status != success`)

### Notes (MVP realism)
- NiFi provides scheduling, run IDs, backpressure, and operational routing.
- Runner is where dbt dependencies live; it is easier to make reproducible than “dbt inside NiFi”.
