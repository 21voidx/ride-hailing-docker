-- Current-state staging for rides, built directly from partitioned CDC events.
-- This avoids querying dev_bronze_cdc_current when its underlying view does not
-- push down a required partition filter to dev_bronze_cdc_events.ride_events.

with source as (
    select *
    from {{ source('dev_bronze_cdc_events', 'ride_events') }}
    where {{ cdc_partition_filter(days_back=30) }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by cast(ride_id as STRING)
            order by
                coalesce(safe_cast(__source_ts_ms as INT64), 0) desc,
                coalesce(safe_cast(__lsn as INT64), 0) desc,
                coalesce(
                    {{ cast_debezium_timestamp('updated_at') }},
                    {{ cast_debezium_timestamp('created_at') }}
                ) desc
        ) as rn
    from source
),

current_rows as (
    select * except (rn)
    from ranked
    where rn = 1
      and coalesce(__op, '') != 'd'
),

cast_ts as (
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
    from current_rows
),

final as (
    select
        *,
        {{ get_jakarta_date('requested_at') }}                  as ride_date
    from cast_ts
)

select * from final