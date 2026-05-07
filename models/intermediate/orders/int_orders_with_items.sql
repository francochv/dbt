with orders as (
    select * from {{ ref('stg_shopify__orders') }}
),

order_items as (
    select * from {{ ref('stg_shopify__order_items') }}
),

items_summary as (
    select
        order_id,
        count(*)                    as line_items_count,
        sum(quantity)               as total_units,
        sum(net_amount)             as items_net_amount,
        sum(item_discount)          as items_total_discount,
        count(distinct product_id)  as distinct_products
    from order_items
    group by order_id
),

final as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_status,
        orders.payment_status,
        orders.fulfillment_status,
        orders.cancel_reason,
        orders.subtotal,
        orders.tax,
        orders.shipping,
        orders.total_price,
        orders.total_discount,
        orders.created_at,
        orders.updated_at,
        orders.cancelled_at,
        orders.closed_at,
        coalesce(items_summary.line_items_count, 0)     as line_items_count,
        coalesce(items_summary.total_units, 0)          as total_units,
        coalesce(items_summary.items_net_amount, 0)     as items_net_amount,
        coalesce(items_summary.items_total_discount, 0) as items_total_discount,
        coalesce(items_summary.distinct_products, 0)    as distinct_products
    from orders
    left join items_summary using (order_id)
)

select * from final
