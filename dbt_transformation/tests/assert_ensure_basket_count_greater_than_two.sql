with basket_counts as (
    select
        dim_product_A_key,
        dim_product_B_key,
        count(*) as basket_count
    from {{ ref('fct_market_basket') }}
    group by dim_product_A_key, dim_product_B_key
)
select * from basket_counts where basket_count <= 2