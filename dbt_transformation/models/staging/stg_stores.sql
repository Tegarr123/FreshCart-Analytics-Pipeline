with source as (
    select 
        store_id,
        store_name,
        store_type
    from {{ source('freshcart_raw', 'stores') }}
)
select * from source