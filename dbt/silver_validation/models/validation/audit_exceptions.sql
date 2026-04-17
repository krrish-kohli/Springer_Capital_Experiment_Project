{{ config(materialized='incremental', alias='audit_exceptions', on_schema_change='sync_all_columns') }}

-- Exception rows generated from var('tables_config_json') (passed by the runner).
-- One row per failing key/record per rule.

with base as (
  select
    toUUID('{{ run_id() }}') as run_id,
    toDateTime('{{ run_ts() }}') as run_ts,
    '{{ config_hash() }}' as config_hash
),

exceptions_unioned as (
  {%- set selects = [] -%}
  {%- for item in iter_tables() -%}
    {%- set dom = item['domain'] -%}
    {%- set t = item['table'] -%}
    {%- set table_name = t.get('name') -%}
    {%- set raw_table = t.get('raw_table') -%}
    {%- set silver_table = t.get('silver_table') -%}
    {%- set pk = t.get('primary_key') -%}

    {%- if t.get('rules', {}).get('duplicates') is not none -%}
      {%- set sql -%}
select
  b.run_id,
  b.run_ts,
  '{{ dom }}' as domain,
  '{{ table_name }}' as table_name,
  '{{ silver_table }}' as silver_table,
  'pk_duplicates' as rule_name,
  {{ pk_to_string_expr(pk, 's') }} as pk,
  'duplicate_pk' as exception_type,
  cast(null as Nullable(String)) as column_name,
  cast(null as Nullable(String)) as bad_value,
  toJSONString(map('note','duplicate_pk')) as row_snapshot,
  b.config_hash
from base b
cross join {{ silver_table }} s
inner join (
  select {{ pk_tuple_expr(pk) }} as pk_val
  from {{ silver_table }}
  group by pk_val
  having count() > 1
) d on d.pk_val = {{ pk_tuple_expr(pk, 's') }}
      {%- endset -%}
      {%- do selects.append(sql) -%}
    {%- endif -%}

    {%- if t.get('rules', {}).get('missing_keys') is not none -%}
      {%- set sql -%}
select
  b.run_id,
  b.run_ts,
  '{{ dom }}' as domain,
  '{{ table_name }}' as table_name,
  '{{ silver_table }}' as silver_table,
  'missing_keys_raw_to_silver' as rule_name,
  {{ pk_to_string_expr(pk, 'r') }} as pk,
  'missing_key' as exception_type,
  cast(null as Nullable(String)) as column_name,
  cast(null as Nullable(String)) as bad_value,
  toJSONString(map('note','missing_in_silver')) as row_snapshot,
  b.config_hash
from base b
cross join {{ raw_table }} r
where {{ pk_tuple_expr(pk, 'r') }} not in (select {{ pk_tuple_expr(pk, 's') }} from {{ silver_table }} s)
      {%- endset -%}
      {%- do selects.append(sql) -%}
    {%- endif -%}

    {%- for fk in (t.get('rules', {}).get('missing_fk', {}).get('checks', [])) -%}
      {%- set sql -%}
select
  b.run_id,
  b.run_ts,
  '{{ dom }}' as domain,
  '{{ table_name }}' as table_name,
  '{{ silver_table }}' as silver_table,
  'missing_fk_{{ fk.get("fk_column") }}' as rule_name,
  {{ pk_to_string_expr(pk, 'o') }} as pk,
  'missing_fk' as exception_type,
  '{{ fk.get("fk_column") }}' as column_name,
  toString(o.{{ fk.get('fk_column') }}) as bad_value,
  toJSONString(map('note','missing_fk')) as row_snapshot,
  b.config_hash
from base b
cross join {{ silver_table }} o
where o.{{ fk.get('fk_column') }} not in (select {{ fk.get('ref_key') }} from {{ fk.get('ref_table') }})
      {%- endset -%}
      {%- do selects.append(sql) -%}
    {%- endfor -%}

    {%- for p in (t.get('rules', {}).get('default_substitution', {}).get('patterns', [])) -%}
      {%- set sql -%}
select
  b.run_id,
  b.run_ts,
  '{{ dom }}' as domain,
  '{{ table_name }}' as table_name,
  '{{ silver_table }}' as silver_table,
  'suspicious_default_substitution' as rule_name,
  {{ pk_to_string_expr(pk, 's') }} as pk,
  'suspicious_default' as exception_type,
  '{{ p.get("column") }}' as column_name,
  toString(s.{{ p.get('column') }}) as bad_value,
  toJSONString(map('note','suspicious_default')) as row_snapshot,
  b.config_hash
from base b
cross join {{ silver_table }} s
where toString(s.{{ p.get('column') }}) in ({% for v in p.get('suspicious_values', []) -%}'{{ v }}'{% if not loop.last %}, {% endif %}{%- endfor %})
      {%- endset -%}
      {%- do selects.append(sql) -%}
    {%- endfor -%}
  {%- endfor -%}

  {%- if selects | length == 0 -%}
    select
      b.run_id,
      b.run_ts,
      'unknown' as domain,
      'unknown' as table_name,
      'unknown' as silver_table,
      'no_tables_in_config' as rule_name,
      'n/a' as pk,
      'config_error' as exception_type,
      cast(null as Nullable(String)) as column_name,
      cast(null as Nullable(String)) as bad_value,
      'Missing tables_config_json var' as row_snapshot,
      b.config_hash
    from base b
  {%- else -%}
    {{ selects | join('\nunion all\n') }}
  {%- endif -%}
)

select *
from exceptions_unioned

{% if is_incremental() %}
where run_id not in (select distinct run_id from {{ this }})
{% endif %}

