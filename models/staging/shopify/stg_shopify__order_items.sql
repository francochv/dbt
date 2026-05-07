with source as (
    select * from {{ source('shopify', 'order_line') }}
),

renamed as (
    select
        -- ids
        id as order_item_id,
        order_id,
        product_id,
        variant_id,

        -- attributes
        title as item_title,
        vendor,
        sku,

        -- quantities & pricing
        quantity,
        price / 100.0 as unit_price,
        total_discount / 100.0 as item_discount,
        (quantity * price / 100.0) - (total_discount / 100.0) as net_amount,

        -- status
        fulfillment_status,
        requires_shipping,
        taxable,

        -- metadata
        _fivetran_synced as _loaded_at

    from source
)

select * from renamed
