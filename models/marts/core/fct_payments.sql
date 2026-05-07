{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        incremental_strategy='delete+insert',
        tags=['critical']
    )
}}

with payments as (
    select * from {{ ref('stg_stripe__payments') }}

    {% if is_incremental() %}
    where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

customers as (
    select customer_id, customer_key from {{ ref('dim_customers') }}
),

invoices as (
    select invoice_id, invoice_status from {{ ref('stg_stripe__invoices') }}
),

dates as (
    select date_day from {{ ref('dim_date') }}
),

final as (
    select
        -- keys
        payments.payment_id,
        customers.customer_key,
        payments.customer_id,
        payments.invoice_id,
        dates.date_day as payment_date_key,

        -- amounts
        payments.amount,
        payments.amount_refunded,
        payments.amount - payments.amount_refunded as net_amount,

        -- status
        payments.payment_status,
        invoices.invoice_status,

        -- derived flags
        payments.payment_status = 'succeeded' as is_successful,
        payments.amount_refunded > 0          as has_refund,

        -- timestamps
        payments.created_at,

        -- metadata
        payments._loaded_at

    from payments
    left join customers on payments.customer_id = customers.customer_id
    left join invoices  on payments.invoice_id  = invoices.invoice_id
    left join dates     on cast(payments.created_at as date) = dates.date_day
)

select * from final
