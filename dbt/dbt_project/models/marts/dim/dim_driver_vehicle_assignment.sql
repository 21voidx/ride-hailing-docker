{{ config(materialized='table') }}
select
    {{ dbt_utils.generate_surrogate_key(['assignment_id', 'valid_from']) }} as assignment_sk,
    assignment_snapshot_id,
    assignment_id,
    driver_id,
    vehicle_id,
    assigned_from,
    assigned_to,
    is_active,
    created_at,
    updated_at,
    valid_from,
    valid_to,
    is_current
from {{ ref('stg_driver_vehicle_assignment') }}
