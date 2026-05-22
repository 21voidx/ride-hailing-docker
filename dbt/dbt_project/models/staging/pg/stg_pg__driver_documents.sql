with source as (
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
_ingested_at,
_source_system
    from {{ source('bronze_pg', 'driver_document') }}
),
deduped as (
    select *
    from source
    qualify row_number() over (partition by document_id order by _ingested_at desc) = 1
)
select
    *,
    {{ audit_columns() }}
from deduped
