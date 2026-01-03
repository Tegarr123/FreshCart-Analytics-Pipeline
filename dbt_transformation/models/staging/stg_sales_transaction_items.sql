with source as (
    select
        item_id,
        transaction_id,
        product_id,
        promotion_id,
        quantity,
        unit_price_at_sale,
        discount_applied,
        line_total,
        updated_at,
        _dlt_load_id
    from {{ source('freshcart_raw', 'sales_transaction_items') }}
)
select * from source