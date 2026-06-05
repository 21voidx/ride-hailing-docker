-- Staging: Ride CDC Status Events
-- Source    : CDC → dev_bronze_cdc_events.ride_events
-- Strategy  : Tidak melakukan dedup per ride_id. Model ini mempertahankan event stream
--             CDC agar perubahan ride_status dapat dianalisis sebagai event lifecycle.
-- Grain     : 1 row per ride status transition dari CDC ride_events.
-- Notes     : stg_ride tetap dipakai untuk current state 1 row per ride.
--             Model ini dipakai oleh fct_ride_status_events.

with source as (
    select *
    from {{ source('ride_ops_cdc', 'ride_events') }}
    where _PARTITIONTIME >= TIMESTAMP('{{ var("cdc_partition_start", "2020-01-01") }}')
),

active as (
    select *
    from source
    where __op != 'd'
      and ride_id is not null
),

parsed as (
    select
        -- CDC event identity
        {{ dbt_utils.generate_surrogate_key([
            "cast(ride_id as string)",
            "cast(__lsn as string)",
            "cast(__source_ts_ms as string)",
            "coalesce(ride_status, '')"
        ]) }} as ride_cdc_event_sk,

        -- keys
        ride_id,
        rider_id,
        driver_id,
        vehicle_id,
        cancelled_by_user_id,

        -- attributes
        ride_status,
        service_type,
        city_code,
        cancel_reason_code,
        cancel_reason_note,

        -- measures
        estimated_distance_km,
        estimated_duration_min,

        -- timestamps
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', requested_at)  as requested_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', accepted_at)   as accepted_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', arrived_at)    as arrived_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', started_at)    as started_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', completed_at)  as completed_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', cancelled_at)  as cancelled_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', created_at)    as created_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', updated_at)    as updated_at,

        -- CDC metadata
        __op                             as _cdc_op,
        __lsn                            as _cdc_lsn,
        timestamp_millis(__source_ts_ms) as _cdc_source_ts

    from active
),

sequenced as (
    select
        *,
        lag(ride_status) over (
            partition by ride_id
            order by _cdc_source_ts, _cdc_lsn
        ) as previous_ride_status
    from parsed
),

status_transitions as (
    select
        {{ dbt_utils.generate_surrogate_key([
            "cast(ride_id as string)",
            "cast(_cdc_lsn as string)",
            "cast(_cdc_source_ts as string)",
            "coalesce(ride_status, '')"
        ]) }} as ride_status_event_id,

        ride_cdc_event_sk,
        ride_id,
        rider_id,
        driver_id,
        vehicle_id,
        cancelled_by_user_id,
        service_type,
        city_code,
        cancel_reason_code,
        cancel_reason_note,

        previous_ride_status as old_status,
        ride_status          as new_status,

        case
            when ride_status = 'REQUESTED'      then requested_at
            when ride_status = 'ACCEPTED'       then accepted_at
            when ride_status = 'ARRIVED'        then arrived_at
            when ride_status = 'IN_PROGRESS'    then started_at
            when ride_status = 'COMPLETED'      then completed_at
            when ride_status = 'PAYMENT_FAILED' then completed_at
            when ride_status = 'CANCELLED'      then cancelled_at
            else coalesce(updated_at, created_at, _cdc_source_ts)
        end as changed_at,

        case
            when ride_status = 'CANCELLED' then cancel_reason_code
            else null
        end as reason_code,

        case
            when ride_status = 'CANCELLED' then cancel_reason_note
            else null
        end as reason_note,

        requested_at,
        accepted_at,
        arrived_at,
        started_at,
        completed_at,
        cancelled_at,
        created_at,
        updated_at,
        _cdc_op,
        _cdc_lsn,
        _cdc_source_ts

    from sequenced
    where previous_ride_status is null
       or previous_ride_status != ride_status
)

select *
from status_transitions
where changed_at is not null
