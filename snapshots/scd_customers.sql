{% snapshot scd_customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select
    customer_id,
    email,
    full_name,
    country_code,
    updated_at
from {{ ref('stg_shopify__customers') }}

{% endsnapshot %}
