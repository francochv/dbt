with source as (
    select * from {{ source('shopify', 'customers') }}
    {{ limit_data_in_dev('created_at') }}
),

renamed as (
    select
        -- ids
        id as customer_id,

        -- contact
        lower(email) as email,
        first_name,
        last_name,
        trim(first_name || ' ' || last_name) as full_name,
        phone,

        -- location
        city,
        province,
        country,
        country_code,

        -- metrics
        orders_count,
        total_spent / 100.0 as total_spent,

        -- flags
        accepts_marketing,
        verified_email,
        tax_exempt,

        -- tags
        tags as customer_tags,

        -- timestamps
        created_at,
        updated_at,

        -- metadata
        _fivetran_synced as _loaded_at

    from source
)

select * from renamed
