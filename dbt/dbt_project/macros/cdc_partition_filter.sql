{% macro cdc_partition_filter(days_back=30) %}
    _PARTITIONTIME >= TIMESTAMP_SUB(
        TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR),
        INTERVAL {{ days_back }} DAY
    )
{% endmacro %}
