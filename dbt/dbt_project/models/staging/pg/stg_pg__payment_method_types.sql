with source as (
    select
        payment_method_type_id,
method_code,
method_name,
is_active,
created_at,
updated_at,
_ingested_at,
_source_system
    from {{ source('bronze_pg', 'payment_method_type') }}
),
deduped as (
    select *
    from source
    qualify row_number() over (partition by payment_method_type_id order by _ingested_at desc) = 1
)
select
    *,
    {{ audit_columns() }}
from deduped
