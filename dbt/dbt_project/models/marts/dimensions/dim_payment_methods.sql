{{ config(cluster_by=['method_code', 'is_active']) }}

select
    payment_method_type_id,
    {{ surrogate_key(["'payment_method_type'", 'payment_method_type_id']) }} as payment_method_key,
    method_code,
    method_name,
    is_active,
    created_at,
    updated_at,
    {{ audit_columns() }}
from {{ ref('stg_pg__payment_method_types') }}
