## dbt project

Project lives in `dbt/silver_validation/`.

### Run locally

```bash
./scripts/run_validation.sh
```

This will:\n+- create a `run_id`\n+- materialize audit outputs (`audit.audit_results`, `audit.audit_exceptions`, marts)\n+- execute dbt tests (non-zero exit when validations fail)\n+
