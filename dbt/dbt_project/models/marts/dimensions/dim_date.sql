{{ config(materialized='table') }}

with date_spine as (
    select date_day
    from unnest(generate_date_array(date('{{ var("reporting_start_date", "2024-01-01") }}'), date_add(current_date('{{ var("timezone", "Asia/Jakarta") }}'), interval 365 day), interval 1 day)) as date_day
)
select
    date_day as date_key,
    {{ surrogate_key(["'date'", 'date_day']) }} as date_surrogate_key,
    date_day,
    extract(year from date_day) as year_number,
    extract(quarter from date_day) as quarter_number,
    extract(month from date_day) as month_number,
    format_date('%B', date_day) as month_name,
    extract(week(monday) from date_day) as week_number,
    date_trunc(date_day, week(monday)) as week_start_date,
    date_trunc(date_day, month) as month_start_date,
    date_trunc(date_day, quarter) as quarter_start_date,
    extract(dayofweek from date_day) as day_of_week_number,
    format_date('%A', date_day) as day_name,
    date_day in unnest([date_sub(current_date('{{ var("timezone", "Asia/Jakarta") }}'), interval 1 day), current_date('{{ var("timezone", "Asia/Jakarta") }}')]) as is_recent_day,
    {{ audit_columns() }}
from date_spine
