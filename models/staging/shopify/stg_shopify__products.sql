with source as (
    select * from {{ source('shopify', 'products') }}
),

renamed as (
    select
        -- ids
        id as product_id,

        -- attributes
        title as product_title,
        vendor,
        product_type,
        handle as product_handle,
        status as product_status,
        tags as product_tags,

        -- timestamps
        created_at,
        updated_at,
        published_at,

        -- metadata
        _fivetran_synced as _loaded_at

    from source
)

select * from renamed
