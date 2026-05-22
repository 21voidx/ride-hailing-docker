select
    user_id,
role_id,
assigned_at,
assigned_by,
is_active,
_ingested_at,
_source_system,
    {{ audit_columns() }}
from {{ source('bronze_pg', 'user_role') }}
