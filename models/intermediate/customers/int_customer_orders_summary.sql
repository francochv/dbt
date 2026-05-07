with shopify_customers as (
    select * from {{ ref('stg_shopify__customers') }}
),

orders as (
    select * from {{ ref('int_orders_with_items') }}
),

orders_summary as (
    select
        customer_id,
        count(*)                                                        as total_orders,
        count(case when order_status = 'closed' then 1 end)            as completed_orders,
        count(case when order_status = 'cancelled' then 1 end)         as cancelled_orders,
        sum(total_price)                                                as gross_revenue,
        sum(total_discount)                                             as total_discounts,
        sum(total_price) - sum(total_discount)                         as net_revenue,
        avg(total_price)                                                as avg_order_value,
        sum(total_units)                                                as total_units_purchased,
        min(created_at)                                                 as first_order_at,
        max(created_at)                                                 as last_order_at,
        datediff('day', min(created_at), max(created_at))              as customer_lifespan_days
    from orders
    group by customer_id
)

select
    shopify_customers.customer_id,
    shopify_customers.email,
    shopify_customers.full_name,
    shopify_customers.country_code,
    shopify_customers.created_at as shopify_created_at,
    coalesce(orders_summary.total_orders, 0)            as total_orders,
    coalesce(orders_summary.completed_orders, 0)        as completed_orders,
    coalesce(orders_summary.cancelled_orders, 0)        as cancelled_orders,
    coalesce(orders_summary.gross_revenue, 0)           as gross_revenue,
    coalesce(orders_summary.total_discounts, 0)         as total_discounts,
    coalesce(orders_summary.net_revenue, 0)             as net_revenue,
    orders_summary.avg_order_value,
    coalesce(orders_summary.total_units_purchased, 0)   as total_units_purchased,
    orders_summary.first_order_at,
    orders_summary.last_order_at,
    orders_summary.customer_lifespan_days
from shopify_customers
left join orders_summary using (customer_id)
