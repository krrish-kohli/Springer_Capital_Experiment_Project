with latest_runs as (
  select
    domain,
    table_name,
    max(run_ts) as latest_run_ts
  from audit.audit_results
  group by domain, table_name
),
latest_results as (
  select r.*
  from audit.audit_results r
  inner join latest_runs lr
    on lr.domain = r.domain
   and lr.table_name = r.table_name
   and lr.latest_run_ts = r.run_ts
),
table_rollup as (
  select
    domain,
    table_name,
    anyLast(silver_table) as silver_table,
    max(status) as worst_status, -- Enum8 ordering: pass<warn<fail<error
    count() as rules_evaluated,
    sum(status = 'fail') as rules_failed,
    sum(status = 'warn') as rules_warn,
    sum(status = 'error') as rules_error,
    max(run_ts) as run_ts
  from latest_results
  group by domain, table_name
)

select *
from table_rollup

