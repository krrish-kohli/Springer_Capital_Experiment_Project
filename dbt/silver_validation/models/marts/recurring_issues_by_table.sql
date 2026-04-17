select
  domain,
  table_name,
  rule_name,
  countIf(status = 'fail') as fail_count,
  countIf(status = 'warn') as warn_count,
  countIf(status = 'error') as error_count,
  max(run_ts) as last_seen_ts
from audit.audit_results
where run_ts >= now() - INTERVAL 30 DAY
group by domain, table_name, rule_name
having fail_count > 0 or warn_count > 0 or error_count > 0
order by fail_count desc, error_count desc, warn_count desc, domain, table_name, rule_name

