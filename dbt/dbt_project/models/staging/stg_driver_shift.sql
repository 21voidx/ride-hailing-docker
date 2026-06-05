select
    cast(shift_id as int64) as shift_id,
    cast(driver_id as int64) as driver_id,
    upper(cast(shift_status as string)) as shift_status,
    cast(started_at as timestamp) as started_at,
    cast(ended_at as timestamp) as ended_at,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at
from {{ source('bronze_pg', 'driver_shift') }}
