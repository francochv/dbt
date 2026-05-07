with orders as (
    select * from {{ ref('int_orders_with_items') }}
    where order_status != 'cancelled'
),

daily as (
    select
        cast(created_at as date)            as revenue_date,
        count(*)                            as orders_count,
        count(distinct customer_id)         as unique_customers,
        sum(total_price)                    as gross_revenue,
        sum(total_discount)                 as total_discounts,
        sum(total_price) - sum(total_discount) as net_revenue,
        sum(total_units)                    as units_sold,
        avg(total_price)                    as avg_order_value
    from orders
    group by 1
)

select * from daily
