{{ config(materialized='table') }}
select
    {{ dbt_utils.generate_surrogate_key(['payment_method_id', 'valid_from']) }} as payment_method_sk,
    payment_method_snapshot_id,
    payment_method_id,
    rider_id,
    method_code,
    provider_name,
    masked_account,
    payment_method_status,
    is_default,
    date(created_at) as payment_method_created_date,
    created_at,
    updated_at,
    valid_from,
    valid_to,
    is_current,
    is_deleted
from {{ ref('stg_payment_method') }}
