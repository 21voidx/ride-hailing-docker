{{ config(
    unique_key='review_id',
    partition_by={'field': 'review_date', 'data_type': 'date'},
    cluster_by=['ride_id', 'review_type']
) }}

select
    review_id,
    ride_id,
    reviewer_id,
    reviewee_id,
    review_type,
    rating_score,
    comments,
    created_at,
    updated_at,
    deleted_at,
    date(created_at, '{{ var("timezone", "Asia/Jakarta") }}') as review_date,
    {{ audit_columns() }}
from {{ ref('stg_pg__reviews') }}
where deleted_at is null
{% if is_incremental() %}
  and date(created_at, '{{ var("timezone", "Asia/Jakarta") }}') >= date_sub(current_date('{{ var("timezone", "Asia/Jakarta") }}'), interval {{ var('incremental_lookback_days', 3) }} day)
{% endif %}
