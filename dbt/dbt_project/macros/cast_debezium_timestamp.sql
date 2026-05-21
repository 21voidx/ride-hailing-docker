{% macro cast_debezium_timestamp(col) %}
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', NULLIF(TRIM({{ col }}), ''))
{% endmacro %}
