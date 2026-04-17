select
  domain,
  table_name,
  silver_table,
  run_ts,
  worst_status,
  rules_failed,
  rules_warn,
  rules_error
from {{ ref('silver_validation_summary') }}
where worst_status in ('fail', 'error')
order by run_ts desc, domain, table_name

