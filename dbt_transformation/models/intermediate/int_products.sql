{{ config(
    incremental_strategy='merge',
    unique_key= 'product_id'
)}}

with products as (
    select
        product_id,
        sku as product_sku,
        name as product_name,
        category_id as product_category_id,
        current_base_price as product_current_base_price,
        row_number() over (
            partition by product_id
            order by updated_at desc, _dlt_load_id desc
        ) as rn,
        updated_at
    from {{ ref('stg_products') }}

    {% if is_incremental() %}
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),
category as (
    select
        category_id as product_category_id,
        category_name as product_category_name
    from {{ ref('stg_product_categories') }}
), product_with_category_deduped as (
    select
        p.product_id,
        p.product_sku,
        p.product_name,
        p.product_category_id,
        c.product_category_name,
        p.product_current_base_price,
        p.updated_at
    from products p
    left join category c
        on p.product_category_id = c.product_category_id
    where p.rn = 1
)
select * from product_with_category_deduped