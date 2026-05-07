with source as (
    select * from {{ source('erp', 'suppliers') }}
),

renamed as (
    select
        -- ids
        supplier_id,

        -- attributes
        supplier_name,
        country_code,

        -- terms
        lead_time_days,
        payment_terms_days,

        -- flags
        is_active,

        -- timestamps
        created_date as created_at,

        -- metadata
        _batch_loaded_at as _loaded_at

    from source
)

select * from renamed
