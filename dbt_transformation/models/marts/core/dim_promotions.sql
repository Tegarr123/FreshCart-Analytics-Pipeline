with promotions as (
    select
        promotion_id,
        promo_code as promotion_code,
        promo_name as promotion_name,
        cast(start_date as date) as promotion_start_date,
        cast(end_date as date) as promotion_end_date,
        d_type as promotion_type
    from {{ ref('stg_promotions') }}
    order by promotion_id
),
non_promotions as (
    select
        0 as promotion_id,
        'NO_PROMO' as promotion_code,
        'EMPTY' as promotion_name,
        '9999-12-01' as promotion_start_date,
        '9999-12-31' as promotion_end_date,
        'EMPTY' as promotion_type
),
combined as (
    select * from non_promotions
    union all
    select * from promotions
),
final as (
    select 
        row_number() over (order by promotion_id) as promotion_key,
        promotion_id,
        promotion_code,
        promotion_name,
        promotion_start_date,
        promotion_end_date,
        promotion_type
    from combined
) select * from final