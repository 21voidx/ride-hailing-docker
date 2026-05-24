-- Staging: Driver Profile
-- Source    : CDC → dev_bronze_cdc_events.driver_profile_events
-- Strategy  : Ambil state terbaru per driver_id (dedup via __lsn DESC)
--             Filter __op = 'd' (deleted records)
-- Notes     : Timestamps dari Debezium disimpan sebagai STRING ISO 8601.
--             Partition filter wajib karena require_partition_filter = TRUE.

with source as (
    select * from {{ source('ride_ops_cdc', 'driver_profile_events') }}
    -- Partition filter wajib: cover semua data historis
    where _PARTITIONTIME >= TIMESTAMP('{{ var("cdc_partition_start", "2020-01-01") }}')
),

-- Ambil 1 event terbaru per driver_id berdasarkan LSN tertinggi
deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by driver_id
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
        driver_id,
        user_id,
        license_number,
        license_expiry,
        driver_status,
        verification_status,
        rating_avg,
        rating_count,

        -- timestamps: cast dari STRING (Debezium ISO 8601) ke TIMESTAMP
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', verified_at)   as verified_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', suspended_at)  as suspended_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', created_at)    as created_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', updated_at)    as updated_at,

        -- derived
        verification_status = 'VERIFIED'  as is_verified,
        driver_status       = 'SUSPENDED' as is_suspended,
        license_expiry < current_date     as is_license_expired,

        -- CDC metadata (audit)
        __op                              as _cdc_op,
        __lsn                             as _cdc_lsn,
        timestamp_millis(__source_ts_ms)  as _cdc_source_ts

    from active
    where driver_id is not null
)

select * from renamed
