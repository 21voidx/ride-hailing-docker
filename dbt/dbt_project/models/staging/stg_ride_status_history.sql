select
    cast(ride_status_history_id as int64) as ride_status_history_id,
    cast(ride_id as int64) as ride_id,
    upper(cast(old_status as string)) as old_status,
    upper(cast(new_status as string)) as new_status,
    upper(cast(changed_by_type as string)) as changed_by_type,
    cast(changed_by_id as int64) as changed_by_id,
    upper(cast(reason_code as string)) as reason_code,
    cast(reason_note as string) as reason_note,
    cast(changed_at as timestamp) as changed_at,
    cast(created_at as timestamp) as created_at
from {{ source('bronze_pg', 'ride_status_history') }}
