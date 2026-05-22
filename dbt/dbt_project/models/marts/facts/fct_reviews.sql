{{ config(
    unique_key='review_id',
    partition_by={'field': 'review_date', 'data_type': 'date'},
    cluster_by=['ride_id', 'review_type']
) }}

select
    review_id,
    {{ surrogate_key(["'review'", 'review_id']) }} as review_key,
    ride_id,
    {{ surrogate_key(["'ride'", 'ride_id']) }} as ride_key,
    reviewer_id,
    {{ surrogate_key(["'user'", 'reviewer_id']) }} as reviewer_key,
    reviewee_id,
    {{ surrogate_key(["'user'", 'reviewee_id']) }} as reviewee_key,
    review_type,
    rating_score,
    comments,
    created_at,
    updated_at,
    deleted_at,
    date(created_at, '{{ var("timezone", "Asia/Jakarta") }}') as review_date,
    {{ surrogate_key(["'date'", "date(created_at, '" ~ var('timezone', 'Asia/Jakarta') ~ "')"]) }} as review_date_key,
    {{ audit_columns() }}
from {{ ref('stg_pg__reviews') }}
where deleted_at is null
{% if is_incremental() %}
  and date(created_at, '{{ var("timezone", "Asia/Jakarta") }}') >= date_sub(current_date('{{ var("timezone", "Asia/Jakarta") }}'), interval {{ var('incremental_lookback_days', 3) }} day)
{% endif %}
