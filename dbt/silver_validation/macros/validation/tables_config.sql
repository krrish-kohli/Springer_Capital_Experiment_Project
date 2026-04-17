{% macro tables_config() %}
  {%- set raw = var('tables_config_json', none) -%}
  {%- if raw is none or raw == '' -%}
    {{ return(none) }}
  {%- endif -%}
  {{ return(fromjson(raw)) }}
{% endmacro %}

{% macro iter_tables() %}
  {%- set cfg = tables_config() -%}
  {%- if cfg is none -%}
    {{ return([]) }}
  {%- endif -%}

  {%- set out = [] -%}
  {%- for dom in cfg.get('domains', []) -%}
    {%- set dom_name = dom.get('name') -%}
    {%- for t in dom.get('tables', []) -%}
      {%- do out.append({'domain': dom_name, 'table': t}) -%}
    {%- endfor -%}
  {%- endfor -%}

  {{ return(out) }}
{% endmacro %}

{% macro pk_tuple_expr(pk_cols, alias=None) %}
  {%- if pk_cols is string -%}
    {%- set cols = [pk_cols] -%}
  {%- else -%}
    {%- set cols = pk_cols -%}
  {%- endif -%}

  {%- if cols | length == 1 -%}
    {%- set c = cols[0] -%}
    {%- if alias -%}
      {{ return(alias ~ '.' ~ c) }}
    {%- else -%}
      {{ return(c) }}
    {%- endif -%}
  {%- endif -%}

  {%- set parts = [] -%}
  {%- for c in cols -%}
    {%- if alias -%}
      {%- do parts.append(alias ~ '.' ~ c) -%}
    {%- else -%}
      {%- do parts.append(c) -%}
    {%- endif -%}
  {%- endfor -%}
  {{ return('tuple(' ~ (parts | join(', ')) ~ ')') }}
{% endmacro %}

{% macro pk_to_string_expr(pk_cols, alias=None) %}
  {%- if pk_cols is string -%}
    {%- set cols = [pk_cols] -%}
  {%- else -%}
    {%- set cols = pk_cols -%}
  {%- endif -%}

  {%- if cols | length == 1 -%}
    {%- set c = cols[0] -%}
    {%- if alias -%}
      {{ return('toString(' ~ alias ~ '.' ~ c ~ ')') }}
    {%- else -%}
      {{ return('toString(' ~ c ~ ')') }}
    {%- endif -%}
  {%- endif -%}

  {# For composite PKs, keep it simple and stable: stringify the PK tuple. #}
  {{ return('toString(' ~ pk_tuple_expr(cols, alias) ~ ')') }}
{% endmacro %}

