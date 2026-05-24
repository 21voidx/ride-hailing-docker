-- Dimension: Date
-- Generated menggunakan dbt_date package
-- Mencakup tanggal 2020-01-01 sampai 2030-12-31
-- Materialized: table (static, rebuild bulanan)

{{
    config(
        materialized='table',
        cluster_by=['full_date']
    )
}}

with date_spine as (
    {{
        dbt_date.get_date_dimension(
            start_date="2020-01-01",
            end_date="2050-12-31"
        )
    }}
),

final as (
    select
        -- surrogate key (integer format YYYYMMDD)
        cast(format_date('%Y%m%d', date_day) as int64)  as date_key,

        -- natural key
        date_day                                         as full_date,

        -- calendar attributes
        extract(year from date_day)                      as year,
        extract(quarter from date_day)                   as quarter_of_year,
        extract(month from date_day)                     as month_of_year,
        format_date('%B', date_day)                      as month_name,
        format_date('%b', date_day)                      as month_name_short,
        extract(week from date_day)                      as week_of_year,
        extract(day from date_day)                       as day_of_month,
        extract(dayofweek from date_day)                 as day_of_week,          -- 1=Sun, 7=Sat
        format_date('%A', date_day)                      as day_name,
        format_date('%a', date_day)                      as day_name_short,

        -- period labels
        format_date('%Y-Q%Q', date_day)                 as year_quarter,          -- e.g. 2024-Q1
        format_date('%Y-%m', date_day)                   as year_month,            -- e.g. 2024-01
        format_date('%Y-W%W', date_day)                  as year_week,             -- e.g. 2024-W01

        -- flags
        extract(dayofweek from date_day) in (1, 7)      as is_weekend,
        extract(dayofweek from date_day) not in (1, 7)  as is_weekday,

        -- Indonesia public holiday flag (seed dari CSV)
        -- extend ini dengan join ke seeds/id_public_holidays.csv
        false                                            as is_public_holiday,
        false                                            as is_long_weekend

    from date_spine
)

select * from final
