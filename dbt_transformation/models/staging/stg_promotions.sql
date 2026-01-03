with source as (
    select
        promotion_id,
        promo_code,
        promo_name,
        start_date,
        end_date,
        d_type,
        discount_value
    from {{ source('freshcart_raw', 'promotions') }}
)
select * from source