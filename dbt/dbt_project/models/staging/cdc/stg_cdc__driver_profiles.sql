with source as (

    select * from {{ source('bronze_cdc_current', 'driver_profile') }}

),

cast_and_clean as (

    select
        driver_id,
        user_id,

        license_number,
        license_expiry,

        driver_status,
        verification_status,

        {{ cast_debezium_timestamp('verified_at') }}   as verified_at,
        {{ cast_debezium_timestamp('suspended_at') }}  as suspended_at,

        rating_avg,
        rating_count,

        {{ cast_debezium_timestamp('created_at') }}    as created_at,
        {{ cast_debezium_timestamp('updated_at') }}    as updated_at,

        (verification_status = 'VERIFIED')             as is_verified,
        (driver_status = 'SUSPENDED')                  as is_suspended,

        __op,
        __table,
        __lsn,
        __source_ts_ms

    from source
    where coalesce(cast(__op as string), '') != 'd'

)

select * from cast_and_clean
