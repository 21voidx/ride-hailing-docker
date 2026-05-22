{% macro is_status(column_name, status_value) -%}
    coalesce(upper(cast({{ column_name }} as string)) = '{{ status_value | upper }}', false)
{%- endmacro %}

{% macro is_not_deleted(cdc_operation_column='cdc_operation') -%}
    coalesce(lower({{ cdc_operation_column }}) != 'd', true)
{%- endmacro %}
