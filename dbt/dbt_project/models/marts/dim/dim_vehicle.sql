{{ config(materialized='table') }}
select
    {{ dbt_utils.generate_surrogate_key(['vehicle_id', 'valid_from']) }} as vehicle_sk,
    vehicle_snapshot_id,
    vehicle_id,
    driver_id,
    license_plate,
    vehicle_type,
    vehicle_make,
    vehicle_model,
    vehicle_year,
    vehicle_status,
    date(created_at) as vehicle_created_date,
    created_at,
    updated_at,
    valid_from,
    valid_to,
    is_current,
    is_deleted
from {{ ref('stg_vehicle') }}
