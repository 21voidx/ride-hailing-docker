{% macro surrogate_key(field_list) -%}
    {#
      Generic deterministic surrogate key for BigQuery.
      Pass a list of SQL expressions. Include an entity namespace as the first
      expression for cross-table uniqueness, for example:
        {{ surrogate_key(["'ride'", 'ride_id']) }}
    #}
    to_hex(md5(concat(
      {%- for field in field_list -%}
        coalesce(cast({{ field }} as string), '_dbt_null_')
        {%- if not loop.last %}, '||', {% endif -%}
      {%- endfor -%}
    )))
{%- endmacro %}

{% macro entity_surrogate_key(entity_name, field_list) -%}
    {{ surrogate_key(["'" ~ entity_name ~ "'"] + field_list) }}
{%- endmacro %}

{% macro ride_surrogate_key(field_list) -%}
    {# Backward-compatible wrapper. Prefer surrogate_key() or entity_surrogate_key(). #}
    {{ surrogate_key(field_list) }}
{%- endmacro %}
