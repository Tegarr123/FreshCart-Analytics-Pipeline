{{ config(
    incremental_strategy='append'
) }}


with sales_transaction_items as (
    select 
        transaction_id,
        product_id,
        coalesce(promotion_id, 0) as promotion_id,
        quantity as item_quantity,
        unit_price_at_sale as item_unit_price_at_sale,
        discount_applied as item_line_discount_applied,
        line_total + discount_applied as item_line_total_before_discount,
        line_total as item_line_total_after_discount,
        updated_at
    from {{ ref('stg_sales_transaction_items') }}

    {% if is_incremental() %}
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),
promotions as (
    select * from {{ ref('stg_promotions') }}
),
products as (
    select * from {{ ref('int_products') }}
),
merged as (
    select 
        sti.transaction_id,
        sti.product_id,
        sti.promotion_id,
        sti.item_quantity,
        sti.item_unit_price_at_sale,
        sti.item_line_discount_applied,
        sti.item_line_total_before_discount,
        sti.item_line_total_after_discount,
        coalesce(p.discount_value, 0.0) as promotion_discount_value,
        (case when p.d_type = 'PERCENTAGE' then 1 else 0 end) as promotion_is_percentage,
        pr.product_current_base_price,
        sti.updated_at
    from sales_transaction_items as sti
    left join promotions as p using (promotion_id)
    left join products as pr using (product_id)
),
final as (
    select
        transaction_id,
        product_id,
        promotion_id,
        sum(item_quantity) as item_quantity,
        avg(item_unit_price_at_sale) as item_unit_price_at_sale,
        sum(item_line_discount_applied) as item_line_discount_applied,
        sum(item_line_total_before_discount) as item_line_total_before_discount,
        sum(item_line_total_after_discount) as item_line_total_after_discount,
        max(promotion_discount_value) as promotion_discount_value,
        max(promotion_is_percentage) as promotion_is_percentage,
        max(product_current_base_price) as product_current_base_price,
        max(updated_at) as updated_at
    from merged
    group by 1,2,3
)

select * from final
order by transaction_id, product_id