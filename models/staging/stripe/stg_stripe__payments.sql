{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        on_schema_change='append_new_columns'
    )
}}

with source as (
    select * from {{ source('stripe', 'payments') }}

    {% if is_incremental() %}
    where _fivetran_synced > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- ids
        id as payment_id,
        customer_id,
        invoice_id,

        -- amounts (convert cents to dollars)
        amount / 100.0 as amount,
        amount_refunded / 100.0 as amount_refunded,

        -- status
        status as payment_status,

        -- timestamps
        created as created_at,

        -- metadata
        _fivetran_synced as _loaded_at

    from source
)

select * from renamed
