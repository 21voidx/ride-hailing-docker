{% macro get_jakarta_date(ts_col) %}
    DATE({{ ts_col }}, 'Asia/Jakarta')
{% endmacro %}
