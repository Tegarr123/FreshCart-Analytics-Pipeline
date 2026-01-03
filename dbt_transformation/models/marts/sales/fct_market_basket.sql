with fct_sales_transactions as (
    select * from {{ ref('fct_sales_transactions') }}
), dim_products as (
    select * from {{ ref('dim_products') }}
),
category_level_pairs as (
    select 
        -- dp1.product_category_name as product_category_1,
        -- dp2.product_category_name as product_category_2,
        dp1.product_category_id as product_category_1_id,
        dp2.product_category_id as product_category_2_id,
        count(distinct fst1.transaction_id) as basket_count,
        count(distinct fst1.transaction_id)::float 
            / nullif((select count(distinct transaction_id) from fct_sales_transactions), 0) as basket_percentage

    from fct_sales_transactions fst1
    join fct_sales_transactions fst2
    on fst1.transaction_id = fst2.transaction_id
    
    join dim_products dp1 on fst1.dim_product_key = dp1.product_key
    join dim_products dp2 on fst2.dim_product_key = dp2.product_key

    where dp1.product_category_id < dp2.product_category_id

    group by 1, 2

    having basket_percentage >= 0.1

    order by basket_count desc
),
product_level_pairs as (
    select 
        fst1.dim_date_key,
        fst1.dim_store_key,
        dp1.product_key as dim_product_A_key,
        dp2.product_key as dim_product_B_key,
        fst1.dim_promotion_key,
        count(distinct fst1.transaction_id) as basket_count,
        sum(fst1.item_quantity) as total_quantity_product_A,
        sum(fst2.item_quantity) as total_quantity_product_B,
        sum(fst1.item_line_total_after_discount) as total_sales_product_A,
        sum(fst2.item_line_total_after_discount) as total_sales_product_B,
        count(distinct fst1.transaction_id)::float 
            / nullif((select count(distinct transaction_id) from fct_sales_transactions), 0) as prod_basket_percentage

    from fct_sales_transactions fst1
    join fct_sales_transactions fst2
    on fst1.transaction_id = fst2.transaction_id
    join dim_products dp1 on fst1.dim_product_key = dp1.product_key
    join dim_products dp2 on fst2.dim_product_key = dp2.product_key

    right join category_level_pairs clp
    on dp1.product_category_id = clp.product_category_1_id
    and dp2.product_category_id = clp.product_category_2_id

    where dp1.product_key < dp2.product_key

    group by 1, 2, 3, 4, 5

    order by dim_date_key, dim_store_key, dim_product_A_key, dim_product_B_key, dim_promotion_key desc
),
final as (

    select 
        dim_date_key,
        dim_product_A_key,
        dim_product_B_key,
        dim_store_key,
        dim_promotion_key,
        basket_count,
        total_quantity_product_A,
        total_quantity_product_B,
        total_sales_product_A,
        total_sales_product_B
    from
    (select
        dim_product_A_key,
        dim_product_B_key,
        count(1) as pair_occurrence_count
    from product_level_pairs
    group by 1, 2
    having pair_occurrence_count > 2) as filtered_pairs
    left join product_level_pairs using (dim_product_A_key, dim_product_B_key)
)
select * from final