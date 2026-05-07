with inventory as (
    select * from {{ ref('stg_erp__inventory') }}
),

products as (
    select * from {{ ref('stg_erp__products') }}
),

latest_snapshot as (
    select
        warehouse_id,
        product_code,
        snapshot_date,
        quantity_on_hand,
        quantity_available,
        quantity_reserved,
        reorder_point,
        reorder_quantity,
        inventory_value
    from inventory
    qualify row_number() over (
        partition by warehouse_id, product_code
        order by snapshot_date desc
    ) = 1
),

final as (
    select
        latest_snapshot.warehouse_id,
        latest_snapshot.product_code,
        products.product_id,
        products.product_name,
        products.category_code,
        products.brand,
        latest_snapshot.snapshot_date,
        latest_snapshot.quantity_on_hand,
        latest_snapshot.quantity_available,
        latest_snapshot.quantity_reserved,
        latest_snapshot.reorder_point,
        latest_snapshot.reorder_quantity,
        latest_snapshot.inventory_value,
        products.unit_cost,
        case
            when latest_snapshot.quantity_available <= 0                              then 'out_of_stock'
            when latest_snapshot.quantity_available <= latest_snapshot.reorder_point then 'low_stock'
            else 'in_stock'
        end as stock_status
    from latest_snapshot
    left join products using (product_code)
)

select * from final
