with source as (
    select * from {{ source('shopify', 'orders') }}
),

renamed as (
    select
        -- ids
        id as order_id,
        customer_id,

        -- status
        status as order_status,
        financial_status as payment_status,
        fulfillment_status,
        cancel_reason,

        -- amounts
        subtotal_price as subtotal,
        total_tax as tax,
        total_shipping_price_set as shipping,
        total_price,
        total_discounts as total_discount,

        -- metrics
        total_line_items_price as item_count,

        -- timestamps
        created_at,
        updated_at,
        cancelled_at,
        closed_at,

        -- metadata
        _fivetran_synced as _loaded_at

    from source
)

select * from renamed
