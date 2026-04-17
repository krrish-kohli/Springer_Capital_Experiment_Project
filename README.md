## Metadata-Driven Silver Validation Pipeline (NiFi + ClickHouse + dbt)

This repo is a mini but realistic MVP for a **metadata-driven validation + audit pipeline** for ClickHouse **silver** tables.

### Why this matters
In real companies, “silver” tables are consumed downstream. Bad silver data creates silent failures and broken decisions. This project demonstrates a practical pattern where:
- every validation run is **auditable** (run IDs, outcomes, exception records)
- validations are **metadata-driven** (add/change rules via config)
- orchestration is handled by a real control plane (NiFi), not just scripts

### Tool roles
- **Apache NiFi**: orchestration/control plane (scheduling, run metadata, routing)
- **Runner service**: lightweight execution layer NiFi calls (runs dbt, writes run summaries)
- **ClickHouse**: raw/silver demo data + **audit system of record**
- **dbt**: validation SQL generation + reporting marts

### Architecture (high level)
NiFi → Runner → dbt → ClickHouse (`raw/silver/audit/marts`)

### Quickstart (local demo)
1. Start services (ClickHouse + NiFi + Runner):

```bash
docker compose up -d
```

2. Create schemas + seed demo data (seed is idempotent):

```bash
./scripts/init_clickhouse.sh
./scripts/seed_demo_data.sh
```

3. Run validation (two options)

Option A (recommended MVP): call the Runner directly:

```bash
curl -sS -X POST http://localhost:8080/runs \
  -H 'Content-Type: application/json' \
  -d '{"scope":{"domains":["customer","sales"],"tables":["customers","orders"]}}' | jq .
```

Option B: run dbt locally (still supported):

```bash
./scripts/run_validation.sh || true
```

4. Query results (audit + marts):

```bash
./scripts/sample_queries.sh
```

### Demo walkthrough (what you should see)
Seeded data intentionally includes issues so validations fail in a meaningful way:
- **PK duplicates** in silver (`customers`, `orders`)
- **Missing keys** from raw → silver
- **Missing FK** (`orders.customer_id` not found in `silver.customers`)
- **Suspicious defaults** (e.g., `UNK`, empty strings, sentinel dates)

You can inspect:
- `audit.audit_run_summary`: run-level status and counts
- `audit.audit_results`: rule-level pass/fail
- `audit.audit_exceptions`: exception records for triage
- `marts.failing_tables_latest`: latest failing tables view

### How to add a new table (MVP pattern)
1. Add a new entry in `config/tables.yml` (domain, raw/silver table names, PK, rules).
2. Ensure the corresponding ClickHouse `raw.*` and `silver.*` tables exist.
3. Trigger a run via runner or NiFi.

### Current MVP limitations (intentional)
- Only a small set of rule types are implemented (drift, duplicates, missing keys/FKs, suspicious defaults).
- dbt schema tests (not_null/unique) are still declared for the sample tables; the core *audit outputs* are driven by metadata.

### Docs
- `docs/architecture.md`
- `docs/validation_rules.md`
- `docs/nifi_flow.md`

