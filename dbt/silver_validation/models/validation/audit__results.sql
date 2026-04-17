{{ config(materialized='incremental', alias='audit_results', on_schema_change='sync_all_columns') }}

-- Rule-level results (one row per rule per table per run).
-- This model is metadata-driven via var('tables_config_json') (passed by the runner).

with base as (
  select
    toUUID('{{ run_id() }}') as run_id,
    toDateTime('{{ run_ts() }}') as run_ts,
    '{{ config_hash() }}' as config_hash
),

results_unioned as (
  {%- set selects = [] -%}
  {%- for item in iter_tables() -%}
    {%- set dom = item['domain'] -%}
    {%- set t = item['table'] -%}
    {%- set table_name = t.get('name') -%}
    {%- set raw_table = t.get('raw_table') -%}
    {%- set silver_table = t.get('silver_table') -%}
    {%- set pk = t.get('primary_key') -%}

    {%- set drift_pct = (t.get('rules', {}).get('row_count', {}).get('max_drift_percent', none)) -%}
    {%- if drift_pct is not none -%}
      {%- set sql -%}
select
  b.run_id,
  b.run_ts,
  '{{ dom }}' as domain,
  '{{ table_name }}' as table_name,
  '{{ silver_table }}' as silver_table,
  'row_count_drift' as rule_name,
  if(stats.raw_cnt = 0, 'pass', if(stats.drift_percent <= {{ drift_pct }}, 'pass', 'fail')) as status_str,
  stats.drift_percent as observed_value,
  cast(null as Nullable(UInt64)) as observed_count,
  {{ drift_pct }} as threshold,
  cast(null as Nullable(Float64)) as expected_min,
  cast(null as Nullable(Float64)) as expected_max,
  toJSONString(map('raw_cnt', toString(stats.raw_cnt), 'silver_cnt', toString(stats.silver_cnt))) as details,
  b.config_hash
from base b
cross join (
  select
    (select count() from {{ raw_table }}) as raw_cnt,
    (select count() from {{ silver_table }}) as silver_cnt,
    if((select count() from {{ raw_table }}) = 0, 0.0, abs((select count() from {{ silver_table }}) - (select count() from {{ raw_table }})) * 100.0 / (select count() from {{ raw_table }})) as drift_percent
) as stats
      {%- endset -%}
      {%- do selects.append(sql) -%}
    {%- endif -%}

    {%- set max_dup = (t.get('rules', {}).get('duplicates', {}).get('max_duplicate_rows', none)) -%}
    {%- if max_dup is not none -%}
      {%- set sql -%}
select
  b.run_id,
  b.run_ts,
  '{{ dom }}' as domain,
  '{{ table_name }}' as table_name,
  '{{ silver_table }}' as silver_table,
  'pk_duplicates' as rule_name,
  if(dup_stats.duplicate_rows <= {{ max_dup }}, 'pass', 'fail') as status_str,
  cast(null as Nullable(Float64)) as observed_value,
  toUInt64(dup_stats.duplicate_rows) as observed_count,
  {{ max_dup }} as threshold,
  cast(null as Nullable(Float64)) as expected_min,
  cast(null as Nullable(Float64)) as expected_max,
  toJSONString(map('duplicate_rows', toString(dup_stats.duplicate_rows))) as details,
  b.config_hash
from base b
cross join (
  select
    ifNull(sum(cnt - 1), 0) as duplicate_rows
  from (
    select {{ pk_tuple_expr(pk) }} as pk_val, count() as cnt
    from {{ silver_table }}
    group by pk_val
    having count() > 1
  )
) as dup_stats
      {%- endset -%}
      {%- do selects.append(sql) -%}
    {%- endif -%}

    {%- set max_missing = (t.get('rules', {}).get('missing_keys', {}).get('max_missing_rows', none)) -%}
    {%- if max_missing is not none -%}
      {%- set sql -%}
select
  b.run_id,
  b.run_ts,
  '{{ dom }}' as domain,
  '{{ table_name }}' as table_name,
  '{{ silver_table }}' as silver_table,
  'missing_keys_raw_to_silver' as rule_name,
  if(mk_stats.missing_rows <= {{ max_missing }}, 'pass', 'fail') as status_str,
  cast(null as Nullable(Float64)) as observed_value,
  toUInt64(mk_stats.missing_rows) as observed_count,
  {{ max_missing }} as threshold,
  cast(null as Nullable(Float64)) as expected_min,
  cast(null as Nullable(Float64)) as expected_max,
  toJSONString(map('missing_rows', toString(mk_stats.missing_rows))) as details,
  b.config_hash
from base b
cross join (
  select
    count() as missing_rows
  from (
    select {{ pk_tuple_expr(pk, 'r') }} as pk_val
    from {{ raw_table }} r
    where {{ pk_tuple_expr(pk, 'r') }} not in (select {{ pk_tuple_expr(pk, 's') }} from {{ silver_table }} s)
  )
) as mk_stats
      {%- endset -%}
      {%- do selects.append(sql) -%}
    {%- endif -%}

    {%- for fk in (t.get('rules', {}).get('missing_fk', {}).get('checks', [])) -%}
      {%- set max_fk = fk.get('max_missing_rows', 0) -%}
      {%- set sql -%}
select
  b.run_id,
  b.run_ts,
  '{{ dom }}' as domain,
  '{{ table_name }}' as table_name,
  '{{ silver_table }}' as silver_table,
  'missing_fk_{{ fk.get("fk_column") }}' as rule_name,
  if(fk_stats.missing_rows <= {{ max_fk }}, 'pass', 'fail') as status_str,
  cast(null as Nullable(Float64)) as observed_value,
  toUInt64(fk_stats.missing_rows) as observed_count,
  {{ max_fk }} as threshold,
  cast(null as Nullable(Float64)) as expected_min,
  cast(null as Nullable(Float64)) as expected_max,
  toJSONString(map('missing_rows', toString(fk_stats.missing_rows))) as details,
  b.config_hash
from base b
cross join (
  select
    count() as missing_rows
  from {{ silver_table }} o
  where o.{{ fk.get('fk_column') }} not in (select {{ fk.get('ref_key') }} from {{ fk.get('ref_table') }})
) as fk_stats
      {%- endset -%}
      {%- do selects.append(sql) -%}
    {%- endfor -%}

    {%- set patterns = (t.get('rules', {}).get('default_substitution', {}).get('patterns', [])) -%}
    {%- if patterns | length > 0 -%}
      {%- set sql -%}
select
  b.run_id,
  b.run_ts,
  '{{ dom }}' as domain,
  '{{ table_name }}' as table_name,
  '{{ silver_table }}' as silver_table,
  'suspicious_default_substitution' as rule_name,
  if(sd_stats.suspicious_rows = 0, 'pass', 'fail') as status_str,
  if(sd_stats.total_rows = 0, 0.0, sd_stats.suspicious_rows * 100.0 / sd_stats.total_rows) as observed_value,
  toUInt64(sd_stats.suspicious_rows) as observed_count,
  cast(null as Nullable(Float64)) as threshold,
  cast(null as Nullable(Float64)) as expected_min,
  cast(null as Nullable(Float64)) as expected_max,
  toJSONString(map('suspicious_rows', toString(sd_stats.suspicious_rows), 'total_rows', toString(sd_stats.total_rows))) as details,
  b.config_hash
from base b
cross join (
  select
    count() as total_rows,
    sum(suspicious_flag) as suspicious_rows
  from (
    select
      {% for p in patterns -%}
        (toString({{ p.get('column') }}) in ({% for v in p.get('suspicious_values', []) -%}'{{ v }}'{% if not loop.last %}, {% endif %}{%- endfor %})) {% if not loop.last %} or {% endif %}
      {%- endfor %} as suspicious_flag
    from {{ silver_table }}
  )
) as sd_stats
      {%- endset -%}
      {%- do selects.append(sql) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- if selects | length == 0 -%}
    select
      b.run_id,
      b.run_ts,
      'unknown' as domain,
      'unknown' as table_name,
      'unknown' as silver_table,
      'no_rules_enabled' as rule_name,
      'error' as status_str,
      cast(null as Nullable(Float64)) as observed_value,
      cast(null as Nullable(UInt64)) as observed_count,
      cast(null as Nullable(Float64)) as threshold,
      cast(null as Nullable(Float64)) as expected_min,
      cast(null as Nullable(Float64)) as expected_max,
      'No rule blocks produced from tables_config_json' as details,
      b.config_hash
    from base b
  {%- else -%}
    {{ selects | join('\nunion all\n') }}
  {%- endif -%}
)

select
  run_id,
  run_ts,
  domain,
  table_name,
  silver_table,
  rule_name,
  CAST(status_str, 'Enum8(''pass'' = 1, ''warn'' = 2, ''fail'' = 3, ''error'' = 4)') as status,
  observed_value,
  observed_count,
  threshold,
  expected_min,
  expected_max,
  details,
  config_hash
from results_unioned

{% if is_incremental() %}
where run_id not in (select distinct run_id from {{ this }})
{% endif %}

