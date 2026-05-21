{{
    config(
        materialized='table',
        tags=['daily']
    )
}}

with driver_enriched as (

    select * from {{ ref('int__driver_enriched') }}

),

final as (

    select
        driver_id,
        user_id,
        username,
        license_number,
        license_expiry,
        driver_status,
        verification_status,
        is_verified,
        is_suspended,
        verified_at,
        suspended_at,
        rating_avg,
        rating_count,
        all_docs_verified,
        verified_doc_count,
        total_doc_count,
        expired_doc_count,
        days_since_verified,
        active_vehicle_id,
        active_vehicle_type,
        active_vehicle_make,
        active_vehicle_model,
        active_vehicle_plate,
        created_at,
        updated_at

    from driver_enriched

)

select * from final
