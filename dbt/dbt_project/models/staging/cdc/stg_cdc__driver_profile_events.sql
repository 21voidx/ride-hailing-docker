select
    cast(driver_id as int64) as driver_id,
    cast(user_id as int64) as user_id,
    license_number,
    cast(license_expiry as date) as license_expiry,
    upper(driver_status) as driver_status,
    upper(verification_status) as verification_status,
    {{ safe_parse_cdc_timestamp('verified_at') }} as verified_at,
    {{ safe_parse_cdc_timestamp('suspended_at') }} as suspended_at,
    cast(rating_avg as numeric) as rating_avg,
    cast(rating_count as int64) as rating_count,
    {{ safe_parse_cdc_timestamp('created_at') }} as created_at,
    {{ safe_parse_cdc_timestamp('updated_at') }} as updated_at,
    lower(__op) as cdc_operation,
    __table as cdc_table,
    cast(__lsn as int64) as cdc_lsn,
    timestamp_millis(cast(__source_ts_ms as int64)) as cdc_event_at,
    _partitiontime as cdc_partition_at,
    {{ audit_columns() }}
from {{ source('bronze_cdc_events', 'driver_profile_events') }}
where _partitiontime >= timestamp_sub(current_timestamp(), interval {{ var('cdc_lookback_hours', 720) }} hour)
