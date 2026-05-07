with source as (
    select * from {{ source('erp', 'products') }}
),

renamed as (
    select
        -- natural keys
        product_code,
        product_id,

        -- attributes
        product_name,
        category_code,
        subcategory_code,
        brand,

        -- financials
        unit_cost,
        unit_price as list_price,

        -- logistics
        weight_kg,
        is_active,

        -- timestamps
        created_date as created_at,
        updated_date as updated_at,

        -- metadata
        _batch_loaded_at as _loaded_at

    from source
)

select * from renamed
