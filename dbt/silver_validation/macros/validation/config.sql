{% macro config_hash() %}
  {{ return(var('config_hash', '')) }}
{% endmacro %}

{% macro run_id() %}
  {{ return(var('run_id', '00000000-0000-0000-0000-000000000000')) }}
{% endmacro %}

{% macro run_ts() %}
  {{ return(var('run_ts', '1970-01-01 00:00:00')) }}
{% endmacro %}

