
with source as (
    select 
        category_id,
        category_name
    from {{ source('freshcart_raw', 'product_categories') }}
)
select * from source