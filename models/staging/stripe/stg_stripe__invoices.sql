{{
    config(
        materialized='incremental',
        unique_key='invoice_id',
        on_schema_change='append_new_columns'
    )
}}

with source as (
    select * from {{ source('stripe', 'invoices') }}

    {% if is_incremental() %}
    where _fivetran_synced > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- ids
        id as invoice_id,
        customer_id,
        subscription_id,

        -- amounts
        {{ cents_to_dollars('amount_due') }} as amount_due,
        {{ cents_to_dollars('amount_paid') }} as amount_paid,
        {{ cents_to_dollars('amount_remaining') }} as amount_remaining,

        -- status
        status as invoice_status,
        currency,

        -- timestamps
        created as created_at,
        period_start,
        period_end,
        due_date,
        paid_at,

        -- metadata
        _fivetran_synced as _loaded_at

    from source
)

select * from renamed
