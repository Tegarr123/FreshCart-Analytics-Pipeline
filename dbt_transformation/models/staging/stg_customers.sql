with source as (
    select 
        customer_id,
        first_name,
        last_name,
        email,
        loyalty_tier,
        updated_at,
        _dlt_load_id
    from {{ source('freshcart_raw', 'customers') }}
)
select *
from source