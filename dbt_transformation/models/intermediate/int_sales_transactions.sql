{{ config(
    incremental_strategy='append'
) }}

with sales_transactions as (
    select 
        transaction_id,
        store_id,
        coalesce(customer_id, 0) as customer_id,
        total_amount as transaction_total_after_tax,
        total_amount - tax_amount as transaction_total_before_tax,
        tax_amount as transaction_tax_amount,
        iff(status = 'COMPLETED', 1, 0) as transaction_completed,
        cast(created_at as date) as transaction_date,
        updated_at
    from {{ ref('stg_sales_transactions') }}

    {% if is_incremental() %}
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)
select * from sales_transactions
order by transaction_id