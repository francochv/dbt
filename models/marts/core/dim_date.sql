{{
    config(
        materialized='table',
        tags=['critical']
    )
}}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('start_date') ~ "' as date)",
        end_date="cast(current_date + INTERVAL 2 YEAR as date)"
    ) }}
),

final as (
    select
        cast(date_day as date) as date_day,

        -- calendar attributes
        year(date_day)         as year,
        quarter(date_day)      as quarter,
        month(date_day)        as month,
        weekofyear(date_day)   as week_of_year,
        dayofweek(date_day)    as day_of_week,
        dayofmonth(date_day)   as day_of_month,
        dayofyear(date_day)    as day_of_year,

        -- labels
        strftime(date_day, '%B') as month_name,
        strftime(date_day, '%A') as day_name,

        -- flags
        dayofweek(date_day) in (0, 6) as is_weekend,

        -- period boundaries
        date_trunc('month',   date_day) as first_day_of_month,
        last_day(date_day)              as last_day_of_month,
        date_trunc('quarter', date_day) as first_day_of_quarter,
        date_trunc('year',    date_day) as first_day_of_year,

        -- composite keys for aggregation
        year(date_day) * 100 + quarter(date_day) as year_quarter,
        year(date_day) * 100 + month(date_day)   as year_month

    from date_spine
)

select * from final
