with int_customers as (
    select 
        customer_id,
        customer_first_name,
        customer_last_name,
        customer_full_name,
        customer_email,
        customer_loyalty_tier
    from {{ ref('int_customers') }}
    order by customer_id
),
anonymoused as (
    select 
        0 as customer_id,
        'ANONYMOUS' as customer_first_name,
        'ANONYMOUS' as customer_last_name,
        'ANONYMOUS' as customer_full_name,
        'EMPTY' as customer_email,
        'EMPTY' as customer_loyalty_tier
), combined as (
    select * from anonymoused
    union all
    select * from int_customers
)
select
    row_number() over (order by customer_id) as customer_key,
    combined.*
from combined