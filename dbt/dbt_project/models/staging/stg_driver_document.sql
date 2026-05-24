-- Staging: Driver Document
-- Source    : Batch → dev_bronze_pg.driver_document
-- Strategy  : Upsert. Satu baris per document_id (state terbaru).
-- Notes     : Timestamps sudah bertipe TIMESTAMP (bukan STRING).
--             Grain: 1 baris per dokumen verifikasi driver.

with source as (
    select * from {{ source('ride_ops_batch', 'driver_document') }}
),

renamed as (
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

        -- derived
        verification_status = 'VERIFIED'  as is_verified,
        verification_status = 'REJECTED'  as is_rejected,
        verification_status = 'PENDING'   as is_pending,
        expires_at < current_date         as is_expired

    from source
    where document_id is not null
)

select * from renamed
