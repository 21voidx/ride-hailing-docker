{% macro cast_debezium_timestamp(column_name) %}
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', {{ column_name }})
{% endmacro %}
