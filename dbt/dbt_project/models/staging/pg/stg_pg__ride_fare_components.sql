select
    fare_component_id,
fare_id,
component_code,
component_name,
component_amount,
description,
created_at,
_ingested_at,
_source_system,
    {{ audit_columns() }}
from {{ source('bronze_pg', 'ride_fare_component') }}
