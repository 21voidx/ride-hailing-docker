{% macro minutes_between(start_ts, end_ts) -%}
    case
      when {{ start_ts }} is null or {{ end_ts }} is null then null
      else timestamp_diff({{ end_ts }}, {{ start_ts }}, second) / 60.0
    end
{%- endmacro %}

{% macro safe_divide(numerator, denominator) -%}
    safe_divide(cast({{ numerator }} as numeric), nullif(cast({{ denominator }} as numeric), 0))
{%- endmacro %}

{% macro jakarta_date(timestamp_expr) -%}
    date({{ timestamp_expr }}, '{{ var("timezone", "Asia/Jakarta") }}')
{%- endmacro %}
