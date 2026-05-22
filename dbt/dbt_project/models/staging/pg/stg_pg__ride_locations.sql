select
    ride_location_id,
ride_id,
location_type,
latitude,
longitude,
address_text,
place_id,
captured_at,
created_at,
_ingested_at,
_source_system,
    {{ audit_columns() }}
from {{ source('bronze_pg', 'ride_location') }}
