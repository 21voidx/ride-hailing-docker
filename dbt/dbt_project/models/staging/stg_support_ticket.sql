select
    cast(ticket_id as int64) as ticket_id,
    cast(ride_id as int64) as ride_id,
    cast(rider_id as int64) as rider_id,
    cast(driver_id as int64) as driver_id,
    upper(cast(ticket_category as string)) as ticket_category,
    upper(cast(ticket_status as string)) as ticket_status,
    upper(cast(priority as string)) as priority,
    cast(opened_at as timestamp) as opened_at,
    cast(resolved_at as timestamp) as resolved_at,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(deleted_at as timestamp) as deleted_at,
    deleted_at is not null as is_deleted
from {{ source('bronze_mysql', 'support_ticket') }}
