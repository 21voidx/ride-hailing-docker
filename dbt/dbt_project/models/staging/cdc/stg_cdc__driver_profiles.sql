with source as (
    select * from {{ source('dev_bronze_cdc_current', 'driver_profile') }}
    where __op != 'd'
),

cast_ts as (
    select
        cast(driver_id as INT64)                                as driver_id,
        cast(user_id as INT64)                                  as user_id,
        license_number,
        SAFE.PARSE_DATE('%Y-%m-%d', license_expiry)             as license_expiry,
        driver_status,
        verification_status,
        {{ cast_debezium_timestamp('verified_at') }}            as verified_at,
        {{ cast_debezium_timestamp('suspended_at') }}           as suspended_at,
        cast(rating_avg as NUMERIC)                             as rating_avg,
        cast(rating_count as INT64)                             as rating_count,
        {{ cast_debezium_timestamp('created_at') }}             as created_at,
        {{ cast_debezium_timestamp('updated_at') }}             as updated_at,
        __op,
        __table,
        cast(__lsn as INT64)                                    as __lsn,
        cast(__source_ts_ms as INT64)                           as __source_ts_ms
    from source
),

final as (
    select
        *,
        verification_status = 'VERIFIED'                        as is_verified,
        driver_status = 'SUSPENDED'                             as is_suspended
    from cast_ts
)

select * from final
