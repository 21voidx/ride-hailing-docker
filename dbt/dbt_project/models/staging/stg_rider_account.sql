with src as (select * from {{ ref('snap_rider_account') }})
select
    cast(dbt_scd_id as string) as rider_snapshot_id,
    cast(rider_id as int64) as rider_id,
    cast(username as string) as username,
    cast(full_name as string) as full_name,
    lower(cast(email as string)) as email,
    cast(phone_number as string) as phone_number,
    upper(cast(account_status as string)) as account_status,
    upper(cast(city_code as string)) as city_code,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(deleted_at as timestamp) as deleted_at,
    cast(dbt_valid_from as timestamp) as valid_from,
    cast(dbt_valid_to as timestamp) as valid_to,
    dbt_valid_to is null as is_current,
    deleted_at is not null as is_deleted
from src
