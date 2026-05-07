{{
    config(
        materialized='table',
        tags=['critical']
    )
}}

with payments as (
    select * from {{ ref('int_payments_pivoted_to_customer') }}
),

orders as (
    select * from {{ ref('int_customer_orders_summary') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['coalesce(orders.customer_id, payments.customer_id)']) }} as customer_key,

        -- natural key
        coalesce(orders.customer_id, payments.customer_id) as customer_id,

        -- attributes
        orders.email,
        orders.full_name,
        orders.country_code,
        orders.shopify_created_at as customer_created_at,

        -- payment metrics (Stripe)
        coalesce(payments.total_payments, 0)     as total_payments,
        coalesce(payments.successful_payments, 0) as successful_payments,
        coalesce(payments.lifetime_value, 0)      as payments_lifetime_value,
        payments.first_payment_at,
        payments.last_payment_at,

        -- order metrics (Shopify)
        coalesce(orders.total_orders, 0)          as total_orders,
        coalesce(orders.completed_orders, 0)      as completed_orders,
        coalesce(orders.net_revenue, 0)           as orders_net_revenue,
        orders.avg_order_value,
        orders.first_order_at,
        orders.last_order_at,
        orders.customer_lifespan_days,

        -- segmentación
        case
            when coalesce(payments.lifetime_value, 0) >= 1000 then 'high'
            when coalesce(payments.lifetime_value, 0) >= 100  then 'medium'
            else 'low'
        end as customer_tier,

        case
            when coalesce(orders.total_orders, 0) = 0                           then 'no_orders'
            when orders.last_order_at >= current_date - INTERVAL 90 DAY         then 'active'
            when orders.last_order_at >= current_date - INTERVAL 365 DAY        then 'at_risk'
            else 'churned'
        end as customer_lifecycle,

        -- metadata
        current_timestamp as _loaded_at

    from payments
    full outer join orders using (customer_id)
)

select * from final
