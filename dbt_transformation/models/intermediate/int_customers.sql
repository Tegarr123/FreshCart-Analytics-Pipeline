{{ config(
    incremental_strategy='merge',
    unique_key= 'customer_id'
) }}
with customers as (
    select  
        customer_id,
        first_name as customer_first_name,
        last_name as customer_last_name,
        first_name || ' ' || last_name as customer_full_name,
        email as customer_email,
        loyalty_tier as customer_loyalty_tier,
        row_number() over (
            partition by customer_id
            order by updated_at desc, _dlt_load_id desc
        ) as rn,
        updated_at
    from {{ ref('stg_customers') }}
    
    {% if is_incremental() %}
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),
deduped as (
    select
        customer_id,
        customer_first_name,
        customer_last_name,
        customer_full_name,
        customer_email,
        customer_loyalty_tier,
        updated_at
    from customers
    where rn = 1
) 
select * from deduped
order by customer_id