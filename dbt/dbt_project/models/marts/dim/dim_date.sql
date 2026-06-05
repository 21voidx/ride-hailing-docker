{{ config(materialized='table') }}
{{ dbt_date.get_date_dimension('2024-01-01', '2050-12-31') }}
