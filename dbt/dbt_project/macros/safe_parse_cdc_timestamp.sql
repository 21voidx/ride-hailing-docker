{% macro safe_parse_cdc_timestamp(column_name) -%}
    case
      when {{ column_name }} is null then null
      when regexp_contains(cast({{ column_name }} as string), r'^\d{16}$') then timestamp_micros(safe_cast({{ column_name }} as int64))
      when regexp_contains(cast({{ column_name }} as string), r'^\d{13}$') then timestamp_millis(safe_cast({{ column_name }} as int64))
      when regexp_contains(cast({{ column_name }} as string), r'^\d{10}$') then timestamp_seconds(safe_cast({{ column_name }} as int64))
      else coalesce(
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', cast({{ column_name }} as string)),
        safe.parse_timestamp('%Y-%m-%d %H:%M:%E*S%Ez', cast({{ column_name }} as string)),
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*S', cast({{ column_name }} as string), '{{ var("timezone", "Asia/Jakarta") }}'),
        safe.parse_timestamp('%Y-%m-%d %H:%M:%E*S', cast({{ column_name }} as string), '{{ var("timezone", "Asia/Jakarta") }}'),
        safe_cast({{ column_name }} as timestamp)
      )
    end
{%- endmacro %}
