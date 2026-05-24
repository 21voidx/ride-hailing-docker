-- Intermediate: driver dengan satu vehicle aktif terbaru
-- Dipakai oleh dim_driver
-- Materialized sebagai ephemeral (CTE inline)
-- Grain: 1 row per driver_id

with drivers as (
    select * from {{ ref('stg_driver_profile') }}
),

users as (
    select * from {{ ref('stg_user_account') }}
),

active_assignments as (
    select *
    from {{ ref('stg_driver_vehicle_assignment') }}
    where is_active = true
    qualify row_number() over (
        partition by driver_id
        order by assigned_from desc, assignment_id desc
    ) = 1
),

vehicles as (
    select * from {{ ref('stg_vehicle') }}
),

driver_with_user as (
    select
        d.driver_id,
        d.user_id,
        u.username,
        u.email,
        u.phone_number,
        d.license_number,
        d.license_expiry,
        d.driver_status,
        d.verification_status,
        d.is_verified,
        d.is_suspended,
        d.is_license_expired,
        d.verified_at,
        d.suspended_at,
        d.rating_avg,
        d.rating_count,
        d.created_at,
        d.updated_at
    from drivers d
    left join users u on d.user_id = u.user_id
),

driver_with_vehicle as (
    select
        dw.*,
        a.vehicle_id            as current_vehicle_id,
        v.license_plate         as current_vehicle_plate,
        v.vehicle_full_name     as current_vehicle_name,
        v.vehicle_type          as current_vehicle_type,
        v.vehicle_color         as current_vehicle_color,
        a.assigned_from         as vehicle_assigned_from
    from driver_with_user dw
    left join active_assignments a on dw.driver_id = a.driver_id
    left join vehicles v           on a.vehicle_id = v.vehicle_id
)

select * from driver_with_vehicle
