with source as (

    select * from {{ source('bronze_cdc_current', 'ride') }}

),

cast_and_clean as (

    select
        ride_id,
        rider_id,
        driver_id,
        vehicle_id,

        ride_status,
        service_type,
        city_code,

        {{ cast_debezium_timestamp('requested_at') }}  as requested_at,
        {{ cast_debezium_timestamp('accepted_at') }}   as accepted_at,
        {{ cast_debezium_timestamp('arrived_at') }}    as arrived_at,
        {{ cast_debezium_timestamp('started_at') }}    as started_at,
        {{ cast_debezium_timestamp('completed_at') }}  as completed_at,
        {{ cast_debezium_timestamp('cancelled_at') }}  as cancelled_at,

        cancelled_by_user_id,
        cancel_reason_code,
        cancel_reason_note,

        estimated_distance_km,
        estimated_duration_min,

        {{ cast_debezium_timestamp('created_at') }}    as created_at,
        {{ cast_debezium_timestamp('updated_at') }}    as updated_at,

        {{ get_jakarta_date(cast_debezium_timestamp('requested_at')) }} as ride_date,

        __op,
        __table,
        __lsn,
        __source_ts_ms

    from source
    where coalesce(cast(__op as string), '') != 'd'

)

select * from cast_and_clean
