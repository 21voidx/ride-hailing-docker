{{ config(cluster_by=['vehicle_type', 'vehicle_status']) }}

select
    vehicle_id,
    {{ surrogate_key(["'vehicle'", 'vehicle_id']) }} as vehicle_key,
    license_plate,
    vehicle_make,
    vehicle_model,
    vehicle_year,
    vehicle_capacity,
    vehicle_color,
    vehicle_type,
    vehicle_status,
    verified_at,
    created_at,
    updated_at,
    deleted_at,
    {{ audit_columns() }}
from {{ ref('stg_pg__vehicles') }}
where deleted_at is null
