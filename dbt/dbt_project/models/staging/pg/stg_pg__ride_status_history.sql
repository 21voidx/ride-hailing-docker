select
    ride_status_history_id,
ride_id,
old_status,
new_status,
changed_by_user_id,
reason_code,
reason_note,
changed_at,
created_at,
_ingested_at,
_source_system,
    {{ audit_columns() }}
from {{ source('bronze_pg', 'ride_status_history') }}
