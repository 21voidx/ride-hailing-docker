select
    cast(tracking_point_id as int64) as tracking_point_id,
    cast(ride_id as int64) as ride_id,
    cast(driver_id as int64) as driver_id,
    cast(latitude as numeric) as latitude,
    cast(longitude as numeric) as longitude,
    cast(speed_kmh as numeric) as speed_kmh,
    cast(recorded_at as timestamp) as recorded_at,
    cast(created_at as timestamp) as created_at
from {{ source('bronze_pg', 'ride_tracking_point') }}
