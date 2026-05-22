{{ config(cluster_by=['account_status']) }}

with rider_roles as (
    select distinct ur.user_id
    from {{ ref('stg_pg__user_roles') }} ur
    join {{ ref('stg_pg__roles') }} roles
      on ur.role_id = roles.role_id
    where roles.role_code = 'RIDER'
      and ur.is_active = true
)
select
    u.user_id as rider_id,
    {{ surrogate_key(["'rider'", 'u.user_id']) }} as rider_key,
    {{ surrogate_key(["'user'", 'u.user_id']) }} as user_key,
    u.account_status,
    u.email_hash,
    u.phone_hash,
    u.email_verified_at is not null as is_email_verified,
    u.phone_verified_at is not null as is_phone_verified,
    u.last_login_at,
    u.created_at,
    u.updated_at,
    u.deleted_at,
    {{ audit_columns() }}
from {{ ref('stg_pg__user_accounts') }} u
join rider_roles rr on u.user_id = rr.user_id
where u.deleted_at is null
