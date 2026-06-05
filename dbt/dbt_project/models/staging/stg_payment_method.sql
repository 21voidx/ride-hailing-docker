with src as (select * from {{ ref('snap_payment_method') }})
select
    cast(dbt_scd_id as string) as payment_method_snapshot_id,
    cast(payment_method_id as int64) as payment_method_id,
    cast(rider_id as int64) as rider_id,
    upper(cast(method_code as string)) as method_code,
    lower(cast(provider_name as string)) as provider_name,
    cast(masked_account as string) as masked_account,
    upper(cast(payment_method_status as string)) as payment_method_status,
    cast(is_default as bool) as is_default,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(deleted_at as timestamp) as deleted_at,
    cast(dbt_valid_from as timestamp) as valid_from,
    cast(dbt_valid_to as timestamp) as valid_to,
    dbt_valid_to is null as is_current,
    deleted_at is not null as is_deleted
from src
