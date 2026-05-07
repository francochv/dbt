{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "snapshot_date", "data_type": "date", "granularity": "day"}
    )
}}

with source as (
    select * from {{ source('erp', 'inventory_snapshots') }}

    {% if is_incremental() %}
    where snapshot_date >= current_date - INTERVAL 3 DAY
    {% endif %}
),

renamed as (
    select
        -- keys
        warehouse_id,
        product_code,
        snapshot_date,

        -- quantities
        quantity_on_hand,
        quantity_reserved,
        quantity_on_hand - quantity_reserved as quantity_available,

        -- reorder signals
        reorder_point,
        reorder_quantity,

        -- financials
        unit_cost,
        quantity_on_hand * unit_cost as inventory_value,

        -- metadata
        _batch_loaded_at as _loaded_at

    from source
)

select * from renamed
