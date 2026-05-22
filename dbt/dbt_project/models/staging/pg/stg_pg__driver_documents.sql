with source as (
    select * from {{ source('dev_bronze_pg', 'driver_document') }}
),

final as (
    select
        document_id,
        driver_id,
        document_type,
        document_number,
        verification_status,
        submitted_at,
        verified_at,
        verified_by,
        expires_at,
        rejection_reason,
        created_at,
        updated_at,
        verification_status = 'VERIFIED'                                        as is_verified,
        (expires_at is not null and expires_at < CURRENT_DATE())                as is_expired
    from source
)

select * from final
