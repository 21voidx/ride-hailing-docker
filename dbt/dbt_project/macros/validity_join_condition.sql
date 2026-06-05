{% macro validity_join_condition(fact_alias, dim_alias, fact_key, dim_key, fact_ts) -%}
    {{ fact_alias }}.{{ fact_key }} = {{ dim_alias }}.{{ dim_key }}
    and {{ fact_alias }}.{{ fact_ts }} >= {{ dim_alias }}.valid_from
    and {{ fact_alias }}.{{ fact_ts }} < coalesce({{ dim_alias }}.valid_to, timestamp('9999-12-31'))
{%- endmacro %}
