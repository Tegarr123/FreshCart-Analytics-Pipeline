with stores as (
    select 
        store_id,
        store_name,
        store_type
    from {{ ref('stg_stores') }}
    order by store_id 
),
final as (
    select 
        row_number() over (order by store_id) as store_key,
        store_id,
        store_name,
        store_type
    from stores
) select * from final