-- Dimension: Vehicle
-- SCD Type 1
-- Materialized: table (batch refresh, daily)

{{
    config(
        materialized='table',
        cluster_by=['vehicle_id']
    )
}}

with vehicles as (
    select * from {{ ref('stg_vehicle') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['vehicle_id']) }} as vehicle_sk,

        -- natural key
        vehicle_id,

        -- attributes
        license_plate,
        vehicle_make,
        vehicle_model,
        vehicle_year,
        vehicle_full_name,
        vehicle_capacity,
        vehicle_color,
        vehicle_type,
        vehicle_status,
        is_active,
        is_deleted,
        verified_at,

        -- derived: age category
        case
            when extract(year from current_date) - vehicle_year <= 2  then 'NEW'
            when extract(year from current_date) - vehicle_year <= 5  then 'RECENT'
            when extract(year from current_date) - vehicle_year <= 10 then 'STANDARD'
            else 'OLD'
        end as vehicle_age_category,

        created_at  as vehicle_created_at,
        updated_at  as vehicle_updated_at

    from vehicles
    where is_deleted = false
)

select * from final
