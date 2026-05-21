with driver_profiles as (

    select * from {{ ref('stg_cdc__driver_profiles') }}

),

users as (

    select * from {{ ref('stg_pg__users') }}

),

active_assignments as (

    select * from {{ ref('stg_pg__driver_vehicle_assignments') }}
    where is_currently_active = true

),

vehicles as (

    select * from {{ ref('stg_pg__vehicles') }}

),

doc_agg as (

    select
        driver_id,
        COUNTIF(is_verified)                              as verified_doc_count,
        COUNT(*)                                          as total_doc_count,
        COUNTIF(not is_verified and not is_expired)       as pending_doc_count,
        COUNTIF(is_expired)                               as expired_doc_count,
        (COUNTIF(not is_verified and not is_expired) = 0
         and COUNTIF(is_expired) = 0
         and COUNT(*) > 0)                                as all_docs_verified

    from {{ ref('stg_pg__driver_documents') }}
    group by 1

),

enriched as (

    select
        dp.driver_id,
        dp.user_id,
        u.username,
        u.email,
        u.phone_number,
        u.account_status                                  as user_account_status,
        u.signup_date,
        u.cohort_month,

        dp.license_number,
        dp.license_expiry,
        dp.driver_status,
        dp.verification_status,
        dp.verified_at,
        dp.suspended_at,
        dp.rating_avg,
        dp.rating_count,
        dp.is_verified,
        dp.is_suspended,
        dp.created_at,
        dp.updated_at,

        IFNULL(da.all_docs_verified, false)               as all_docs_verified,
        IFNULL(da.verified_doc_count, 0)                  as verified_doc_count,
        IFNULL(da.total_doc_count, 0)                     as total_doc_count,
        IFNULL(da.expired_doc_count, 0)                   as expired_doc_count,

        DATE_DIFF(CURRENT_DATE('Asia/Jakarta'),
                  {{ get_jakarta_date('dp.verified_at') }},
                  DAY)                                    as days_since_verified,

        aa.vehicle_id                                     as active_vehicle_id,
        v.vehicle_type                                    as active_vehicle_type,
        v.vehicle_make                                    as active_vehicle_make,
        v.vehicle_model                                   as active_vehicle_model,
        v.license_plate                                   as active_vehicle_plate,

        dp.__source_ts_ms

    from driver_profiles  dp
    left join users        u   on dp.user_id    = u.user_id
    left join active_assignments aa on dp.driver_id = aa.driver_id
    left join vehicles     v   on aa.vehicle_id = v.vehicle_id
    left join doc_agg      da  on dp.driver_id  = da.driver_id

)

select * from enriched
