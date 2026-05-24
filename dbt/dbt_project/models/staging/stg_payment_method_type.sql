-- Staging: Payment Method Type
-- Source    : Batch → dev_bronze_pg.payment_method_type
-- Strategy  : Upsert. Low-cardinality reference table.
-- Notes     : Timestamps sudah bertipe TIMESTAMP (bukan STRING).

with source as (
    select * from {{ source('ride_ops_batch', 'payment_method_type') }}
),

renamed as (
    select
        payment_method_type_id,
        method_code,
        method_name,
        is_active,
        created_at,
        updated_at

    from source
    where payment_method_type_id is not null
)

select * from renamed
