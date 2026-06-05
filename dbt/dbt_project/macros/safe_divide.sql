{% macro safe_divide(numerator, denominator) -%}
    safe_divide({{ numerator }}, nullif({{ denominator }}, 0))
{%- endmacro %}
