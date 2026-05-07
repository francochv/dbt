{% snapshot scd_products %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_code',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select
    product_code,
    product_name,
    category_code,
    unit_cost,
    list_price,
    is_active,
    updated_at
from {{ ref('stg_erp__products') }}

{% endsnapshot %}
