select
    cast(ride_location_id as int64) as ride_location_id,
    cast(ride_id as int64) as ride_id,
    upper(cast(location_type as string)) as location_type,
    cast(latitude as numeric) as latitude,
    cast(longitude as numeric) as longitude,
    cast(address_text as string) as address_text,
    cast(captured_at as timestamp) as captured_at,
    cast(created_at as timestamp) as created_at
from {{ source('bronze_pg', 'ride_location') }}
