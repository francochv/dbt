{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "snapshot_date", "data_type": "date", "granularity": "day"},
        tags=['supply_chain']
    )
}}

with inventory as (
    select * from {{ ref('int_inventory_stock_levels') }}

    {% if is_incremental() %}
    where snapshot_date >= current_date - INTERVAL 3 DAY
    {% endif %}
),

products as (
    select product_code, product_key from {{ ref('dim_products') }}
),

final as (
    select
        -- keys
        products.product_key,
        inventory.product_code,
        inventory.warehouse_id,
        inventory.snapshot_date,

        -- quantities
        inventory.quantity_on_hand,
        inventory.quantity_available,
        inventory.quantity_reserved,

        -- financials
        inventory.unit_cost,
        inventory.inventory_value,

        -- reorder signals
        inventory.reorder_point,
        inventory.reorder_quantity,
        inventory.stock_status,

        -- metadata
        current_timestamp as _loaded_at

    from inventory
    left join products on inventory.product_code = products.product_code
)

select * from final
