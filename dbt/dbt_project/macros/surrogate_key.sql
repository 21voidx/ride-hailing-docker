{% macro ride_surrogate_key(field_list) -%}
    to_hex(md5(concat(
      {%- for field in field_list -%}
        coalesce(cast({{ field }} as string), '_dbt_null_')
        {%- if not loop.last %}, '||', {% endif -%}
      {%- endfor -%}
    )))
{%- endmacro %}
