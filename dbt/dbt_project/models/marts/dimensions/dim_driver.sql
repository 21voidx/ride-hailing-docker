-- Dimension: Driver
-- SCD Type 1 — overwrite on change
-- Untuk SCD Type 2 (histori perubahan rating/status), gunakan dbt snapshot
-- Materialized: table (CDC refresh, per jam)

{{
    config(
        materialized='table',
        cluster_by=['driver_id']
    )
}}

with drivers as (
    select * from {{ ref('int_driver_with_vehicle') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['driver_id']) }} as driver_sk,

        -- natural key
        driver_id,
        user_id,

        -- identity
        username                    as driver_username,
        email                       as driver_email,
        phone_number                as driver_phone,

        -- license
        license_number,
        license_expiry,
        is_license_expired,

        -- status
        driver_status,
        verification_status,
        is_verified,
        is_suspended,
        verified_at,
        suspended_at,

        -- performance
        rating_avg,
        rating_count,

        -- rating tier (derived)
        case
            when rating_avg >= 4.8 then 'EXCELLENT'
            when rating_avg >= 4.5 then 'GOOD'
            when rating_avg >= 4.0 then 'AVERAGE'
            else 'BELOW_AVERAGE'
        end                         as rating_tier,

        -- current vehicle
        current_vehicle_id,
        current_vehicle_plate,
        current_vehicle_name,
        current_vehicle_type,
        current_vehicle_color,
        vehicle_assigned_from,

        -- timestamps
        created_at                  as driver_created_at,
        updated_at                  as driver_updated_at

    from drivers
)

select * from final
