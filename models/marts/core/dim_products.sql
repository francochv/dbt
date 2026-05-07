{{
    config(
        materialized='table',
        tags=['critical']
    )
}}

with erp_products as (
    select * from {{ ref('stg_erp__products') }}
),

shopify_products as (
    select * from {{ ref('stg_shopify__products') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['erp_products.product_code']) }} as product_key,

        -- natural keys
        erp_products.product_code,
        erp_products.product_id     as erp_product_id,
        shopify_products.product_id as shopify_product_id,

        -- attributes
        erp_products.product_name,
        erp_products.category_code,
        erp_products.subcategory_code,
        erp_products.brand,
        shopify_products.product_type,
        shopify_products.product_status,

        -- financials
        erp_products.unit_cost,
        erp_products.list_price,
        erp_products.list_price - erp_products.unit_cost as gross_margin,
        round(
            (erp_products.list_price - erp_products.unit_cost)
            / nullif(erp_products.list_price, 0) * 100,
            2
        ) as margin_pct,

        -- logistics
        erp_products.weight_kg,
        erp_products.is_active,

        -- timestamps
        erp_products.created_at,
        current_timestamp as _loaded_at

    from erp_products
    left join shopify_products
        on erp_products.product_code = shopify_products.product_handle
)

select * from final
