-- Append-only CDC event log for rides.
-- JANGAN filter __op = 'd' — log harus utuh.
with source as (
    select *
    from {{ source('dev_bronze_cdc_events', 'ride_events') }}
    where {{ cdc_partition_filter(days_back=60) }}
),

final as (
    select
        cast(ride_id as INT64)                                  as ride_id,
        cast(rider_id as INT64)                                 as rider_id,
        cast(driver_id as INT64)                                as driver_id,
        cast(vehicle_id as INT64)                               as vehicle_id,
        ride_status,
        service_type,
        city_code,
        {{ cast_debezium_timestamp('requested_at') }}           as requested_at,
        {{ cast_debezium_timestamp('accepted_at') }}            as accepted_at,
        {{ cast_debezium_timestamp('arrived_at') }}             as arrived_at,
        {{ cast_debezium_timestamp('started_at') }}             as started_at,
        {{ cast_debezium_timestamp('completed_at') }}           as completed_at,
        {{ cast_debezium_timestamp('cancelled_at') }}           as cancelled_at,
        cast(cancelled_by_user_id as INT64)                     as cancelled_by_user_id,
        cancel_reason_code,
        cancel_reason_note,
        cast(estimated_distance_km as NUMERIC)                  as estimated_distance_km,
        cast(estimated_duration_min as NUMERIC)                 as estimated_duration_min,
        {{ cast_debezium_timestamp('created_at') }}             as created_at,
        {{ cast_debezium_timestamp('updated_at') }}             as updated_at,
        __op,
        __table,
        cast(__lsn as INT64)                                    as __lsn,
        cast(__source_ts_ms as INT64)                           as __source_ts_ms
    from source
)

select * from final
