{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        merge_update_columns=['order_status', 'fulfillment_status', 'payment_status',
                              'total_price', 'total_discount', 'updated_at', '_loaded_at'],
        tags=['critical']
    )
}}

with orders as (
    select * from {{ ref('int_orders_with_items') }}

    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

customers as (
    select customer_id, customer_key from {{ ref('dim_customers') }}
),

dates as (
    select date_day from {{ ref('dim_date') }}
),

final as (
    select
        -- keys
        orders.order_id,
        customers.customer_key,
        orders.customer_id,
        dates.date_day as order_date_key,

        -- status
        orders.order_status,
        orders.payment_status,
        orders.fulfillment_status,
        orders.cancel_reason,

        -- measures
        orders.subtotal,
        orders.tax,
        orders.shipping,
        orders.total_price,
        orders.total_discount,
        orders.line_items_count,
        orders.total_units,
        orders.items_net_amount,
        orders.distinct_products,

        -- timestamps
        orders.created_at,
        orders.updated_at,
        orders.cancelled_at,
        orders.closed_at,

        -- metadata
        current_timestamp as _loaded_at

    from orders
    left join customers on orders.customer_id = customers.customer_id
    left join dates on cast(orders.created_at as date) = dates.date_day
)

select * from final
