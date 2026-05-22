{{ config(materialized='view') }}

select
    dva.assignment_id,
    {{ surrogate_key(["'driver_vehicle_assignment'", 'dva.assignment_id']) }} as driver_vehicle_assignment_key,
    dva.driver_id,
    {{ surrogate_key(["'driver'", 'dva.driver_id']) }} as driver_key,
    dva.vehicle_id,
    {{ surrogate_key(["'vehicle'", 'dva.vehicle_id']) }} as vehicle_key,
    dva.assigned_from,
    dva.assigned_to,
    dva.is_active,
    v.vehicle_type,
    v.vehicle_make,
    v.vehicle_model,
    v.vehicle_year,
    v.vehicle_capacity,
    v.vehicle_color,
    v.vehicle_status,
    {{ audit_columns() }}
from {{ ref('stg_pg__driver_vehicle_assignments') }} dva
left join {{ ref('stg_pg__vehicles') }} v
  on dva.vehicle_id = v.vehicle_id
where dva.is_active = true
  and v.deleted_at is null
qualify row_number() over (partition by dva.driver_id order by dva.assigned_from desc, dva.assignment_id desc) = 1
