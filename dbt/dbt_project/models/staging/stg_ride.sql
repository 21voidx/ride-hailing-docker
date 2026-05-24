-- Staging: Ride
-- Source    : CDC → dev_bronze_cdc_events.ride_events
-- Strategy  : Ambil state terbaru per ride_id (dedup via __lsn DESC)
--             Filter __op = 'd' (deleted records)
-- Notes     : Timestamps dari Debezium disimpan sebagai STRING ISO 8601.
--             Partition filter wajib karena require_partition_filter = TRUE.

with source as (
    select * from {{ source('ride_ops_cdc', 'ride_events') }}
    -- Partition filter wajib: cover semua data historis
    where _PARTITIONTIME >= TIMESTAMP('{{ var("cdc_partition_start", "2020-01-01") }}')
),

-- Ambil 1 event terbaru per ride_id berdasarkan LSN tertinggi
-- Ini merepresentasikan state aktual di source PostgreSQL
deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by ride_id
        order by __lsn desc, __source_ts_ms desc
    ) = 1
),

-- Hanya proses record yang tidak di-delete
active as (
    select * from deduplicated
    where __op != 'd'
),

renamed as (
    select
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

        -- timestamps: cast dari STRING (Debezium ISO 8601) ke TIMESTAMP
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', requested_at)  as requested_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', accepted_at)   as accepted_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', arrived_at)    as arrived_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', started_at)    as started_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', completed_at)  as completed_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', cancelled_at)  as cancelled_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', created_at)    as created_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', updated_at)    as updated_at,

        -- CDC metadata (audit)
        __op                                                          as _cdc_op,
        __lsn                                                         as _cdc_lsn,
        timestamp_millis(__source_ts_ms)                              as _cdc_source_ts

    from active
    where ride_id is not null
)

select * from renamed
