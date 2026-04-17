## Architecture (MVP)

### End-to-end flow
1. **NiFi** triggers a validation run (schedule or manual) and generates `run_id` + `run_ts`.
2. NiFi calls the **Runner service** (`POST /runs`) with run metadata and optional scope.
3. **Runner**:
   - reads `config/tables.yml`
   - computes `config_hash`
   - runs dbt with `--vars` containing run metadata and the table config (as JSON)
   - writes `audit.audit_run_summary` start/end rows in ClickHouse
4. **dbt** materializes:
   - rule-level results into `audit.audit_results`
   - exception rows into `audit.audit_exceptions`
   - reporting views into `marts.*`

### Stores (ClickHouse)
- **Raw**: `raw.*` (demo ingestion)
- **Silver**: `silver.*` (validated tables)
- **Audit system of record**:
  - `audit.audit_run_summary` (one row per run)
  - `audit.audit_results` (rule-level outcomes)
  - `audit.audit_exceptions` (row/key-level exceptions)
- **Reporting marts**:
  - `marts.silver_validation_summary`
  - `marts.failing_tables_latest`
  - `marts.recurring_issues_by_table`

### How metadata influences validation
`config/tables.yml` defines, per silver table:
- raw→silver mapping
- primary key
- required columns
- rule thresholds (rowcount drift, duplicates, missing keys/FKs, suspicious defaults)

Runner passes the relevant config subset to dbt as a JSON var, and dbt macros generate the validation SQL from that metadata.

