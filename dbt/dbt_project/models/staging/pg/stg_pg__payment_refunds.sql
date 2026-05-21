with source as (

    select * from {{ source('bronze_pg', 'payment_refund') }}

),

renamed as (

    select
        refund_id,
        transaction_id,
        provider_refund_id,
        refund_amount,
        currency_code,
        refund_status,
        refund_reason_code,
        refund_reason_note,
        requested_at,
        completed_at,
        created_at,
        updated_at,

        (refund_status = 'COMPLETED') as is_completed,

        _ingested_at,
        _source_system

    from source

)

select * from renamed
