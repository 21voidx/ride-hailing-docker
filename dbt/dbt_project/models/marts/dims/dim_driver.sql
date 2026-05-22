with enriched as (
    select * from {{ ref('int__driver_enriched') }}
),

final as (
    select
        driver_id,
        user_id,
        username,
        driver_status,
        verification_status,
        is_verified,
        is_suspended,
        verified_at,
        suspended_at,
        rating_avg,
        rating_count,
        license_number,
        license_expiry,
        active_vehicle_id,
        active_vehicle_type,
        active_vehicle_plate,
        verified_doc_count,
        total_doc_count,
        expired_doc_count,
        all_docs_verified,
        CAST(DATE_DIFF(
            CURRENT_DATE(),
            DATE(verified_at, 'Asia/Jakarta'),
            DAY
        ) AS INT64)                                                     as days_since_verified,
        created_at,
        updated_at
    from enriched
)

select * from final
