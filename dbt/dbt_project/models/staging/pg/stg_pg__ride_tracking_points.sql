select
    tracking_point_id,
ride_id,
driver_id,
latitude,
longitude,
speed_kmh,
heading_degree,
accuracy_meter,
recorded_at,
created_at,
_ingested_at,
_source_system,
    {{ audit_columns() }}
from {{ source('bronze_pg', 'ride_tracking_point') }}
