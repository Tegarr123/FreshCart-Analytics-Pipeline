with int_sales_transactions as (
    select * from {{ ref('int_sales_transactions') }}
),
dim_stores as (
    select * from {{ ref('dim_stores') }}
),
dim_customers as (
    select * from {{ ref('dim_customers') }}
),
dim_date as (
    select * from {{ ref('dim_date') }}
),
enriched_sales_transactions as (
    select 
        ist.transaction_id,
        dd.date_key as dim_date_key,
        ds.store_key as dim_store_key,
        dc.customer_key as dim_customer_key,
        ist.transaction_completed as transaction_is_completed
    from int_sales_transactions as ist
    left join dim_stores as ds using (store_id)
    left join dim_customers as dc using (customer_id)
    left join dim_date as dd on ist.transaction_date = dd.date
),
int_sales_transaction_items as (
    select * from {{ ref('int_sales_transaction_items') }}
),
dim_products as (
    select * from {{ ref('dim_products') }}
),
dim_promotions as (
    select * from {{ ref('dim_promotions') }}
),
enriched_transaction_items as (
    select 
        isti.transaction_id,
        dp.product_key as dim_product_key,
        dpr.promotion_key as dim_promotion_key,
        isti.item_quantity,
        isti.item_unit_price_at_sale,
        isti.item_line_discount_applied,
        isti.item_line_total_before_discount,
        isti.item_line_total_after_discount,
        isti.promotion_discount_value,
        isti.promotion_is_percentage,
        isti.product_current_base_price
    from int_sales_transaction_items as isti
    left join dim_products as dp using (product_id)
    left join dim_promotions as dpr using (promotion_id)
),
final as (
    select 
        est.transaction_id,
        est.dim_date_key,
        est.dim_store_key,
        est.dim_customer_key,
        eti.dim_product_key,
        eti.dim_promotion_key,
        est.transaction_is_completed,
        eti.item_quantity,
        eti.item_unit_price_at_sale,
        eti.item_line_discount_applied,
        eti.item_line_total_before_discount,
        eti.item_line_total_after_discount,
        eti.promotion_discount_value,
        eti.promotion_is_percentage,
        eti.product_current_base_price
    from enriched_sales_transactions as est
    left join enriched_transaction_items as eti using (transaction_id)
)
select * from final
