-- dbt-clickhouse (in this project configuration) creates databases named like `${database}_${schema}`.
-- We create those databases, then expose stable views in `audit.*` and `marts.*` per the project design.

CREATE DATABASE IF NOT EXISTS default_audit;
CREATE DATABASE IF NOT EXISTS default_marts;

-- Stable views (so downstream queries can use audit.* and marts.*)
CREATE OR REPLACE VIEW audit.audit_results AS
SELECT * FROM default_audit.audit_results;

CREATE OR REPLACE VIEW audit.audit_exceptions AS
SELECT * FROM default_audit.audit_exceptions;

CREATE OR REPLACE VIEW marts.silver_validation_summary AS
SELECT * FROM default_marts.silver_validation_summary;

CREATE OR REPLACE VIEW marts.failing_tables_latest AS
SELECT * FROM default_marts.failing_tables_latest;

CREATE OR REPLACE VIEW marts.recurring_issues_by_table AS
SELECT * FROM default_marts.recurring_issues_by_table;

