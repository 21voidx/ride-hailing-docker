with source as (
    select
        role_id,
role_code,
role_name,
description,
is_active,
created_at,
updated_at,
_ingested_at,
_source_system
    from {{ source('bronze_pg', 'role') }}
),
deduped as (
    select *
    from source
    qualify row_number() over (partition by role_id order by _ingested_at desc) = 1
)
select
    *,
    {{ audit_columns() }}
from deduped
