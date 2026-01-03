with source as (
    select
        product_id,
        sku,
        name,
        category_id,
        current_base_price,
        updated_at,
        _dlt_load_id
    from {{ source('freshcart_raw', 'products') }}
)

select * from source