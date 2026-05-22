with driver_profiles as (
    select * from {{ ref('stg_cdc__driver_profiles') }}
),

users as (
    select * from {{ ref('stg_pg__users') }}
),

active_assignments as (
    select *
    from {{ ref('stg_pg__driver_vehicle_assignments') }}
    where is_currently_active = true
),

vehicles as (
    select * from {{ ref('stg_pg__vehicles') }}
),

doc_summary as (
    select
        driver_id,
        COUNTIF(is_verified = true)     as verified_doc_count,
        COUNT(*)                        as total_doc_count,
        COUNTIF(is_expired = true)      as expired_doc_count,
        (COUNTIF(is_verified = true) = COUNT(*)
         and COUNT(*) > 0)              as all_docs_verified
    from {{ ref('stg_pg__driver_documents') }}
    group by driver_id
),

final as (
    select
        dp.driver_id,
        dp.user_id,
        u.username,
        dp.license_number,
        dp.license_expiry,
        dp.driver_status,
        dp.verification_status,
        dp.is_verified,
        dp.is_suspended,
        dp.verified_at,
        dp.suspended_at,
        dp.rating_avg,
        dp.rating_count,
        dp.created_at,
        dp.updated_at,
        dp.__source_ts_ms,

        -- active vehicle
        av.assignment_id                as active_assignment_id,
        v.vehicle_id                    as active_vehicle_id,
        v.vehicle_type                  as active_vehicle_type,
        v.license_plate                 as active_vehicle_plate,

        -- document summary
        COALESCE(ds.verified_doc_count, 0)  as verified_doc_count,
        COALESCE(ds.total_doc_count, 0)     as total_doc_count,
        COALESCE(ds.expired_doc_count, 0)   as expired_doc_count,
        COALESCE(ds.all_docs_verified, false) as all_docs_verified
    from driver_profiles dp
    left join users u            on dp.user_id = u.user_id
    left join active_assignments av on dp.driver_id = av.driver_id
    left join vehicles v         on av.vehicle_id = v.vehicle_id
    left join doc_summary ds     on dp.driver_id = ds.driver_id
)

select * from final
