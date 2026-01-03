with int_products as (
    select * from {{ ref('int_products') }}
    order by product_id
)
select
    row_number() over (order by product_id) as product_key,
    product_id,
    product_sku,
    product_name,
    product_category_id,
    product_category_name
from int_products