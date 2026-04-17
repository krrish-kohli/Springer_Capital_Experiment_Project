## Validation rules (MVP)

### Rule: Required columns not null
Implemented via dbt schema tests in `dbt/silver_validation/models/schema.yml` for:
- `stg_silver__customers`
- `stg_silver__orders`

### Rule: PK uniqueness / duplicates
Implemented two ways:
- dbt schema `unique` test on PK columns (sample tables)
- exceptions materialized into `audit.audit_exceptions` by the metadata-driven model:
  - `dbt/silver_validation/models/validation/audit_exceptions.sql`

### Rule: Missing keys (raw → silver)
- Exceptions generated from metadata in `config/tables.yml` by:
  - `dbt/silver_validation/models/validation/audit_exceptions.sql`

### Rule: Missing FK (orders.customer_id → customers.customer_id)
- Exceptions generated from metadata in `config/tables.yml` by:
  - `dbt/silver_validation/models/validation/audit_exceptions.sql`

### Rule: Suspicious default substitution
- Exceptions generated from `rules.default_substitution.patterns` in `config/tables.yml` by:
  - `dbt/silver_validation/models/validation/audit_exceptions.sql`
