with source as (
    select
        transaction_id,
        store_id,
        customer_id,
        total_amount,
        tax_amount,
        status,
        created_at,
        updated_at,
        _dlt_load_id
    from {{ source('freshcart_raw', 'sales_transactions') }}
)
select * from source