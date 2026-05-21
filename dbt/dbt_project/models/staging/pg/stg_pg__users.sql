with source as (

    select * from {{ source('bronze_pg', 'user_account') }}

),

renamed as (

    select
        user_id,
        user_id                                             as rider_id,
        username,
        email,
        phone_number,
        account_status,
        email_verified_at,
        phone_verified_at,
        last_login_at,
        created_at,
        updated_at,
        deleted_at,

        {{ get_jakarta_date('created_at') }}                as signup_date,
        DATE_TRUNC({{ get_jakarta_date('created_at') }}, MONTH) as cohort_month,

        _ingested_at,
        _source_system

    from source

)

select * from renamed
