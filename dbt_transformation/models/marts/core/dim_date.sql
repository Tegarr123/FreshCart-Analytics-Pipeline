with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2021-01-01' as date)",
        end_date="cast('2027-12-31' as date)"
    ) }}
),
final as (

select
    row_number() over (order by date_day) as date_key,
    date_day as date,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    case 
        when extract(month from date_day) = 1 then 'January'
        when extract(month from date_day) = 2 then 'February'
        when extract(month from date_day) = 3 then 'March'
        when extract(month from date_day) = 4 then 'April'
        when extract(month from date_day) = 5 then 'May'
        when extract(month from date_day) = 6 then 'June'
        when extract(month from date_day) = 7 then 'July'
        when extract(month from date_day) = 8 then 'August'
        when extract(month from date_day) = 9 then 'September'
        when extract(month from date_day) = 10 then 'October'
        when extract(month from date_day) = 11 then 'November'
        when extract(month from date_day) = 12 then 'December'
    end as month_name, 
    extract(quarter from date_day) as quarter,
    extract(dayofweek from date_day) as day_of_week,
    case 
        when extract(dayofweek from date_day) = 0 then 'Sunday'
        when extract(dayofweek from date_day) = 1 then 'Monday'
        when extract(dayofweek from date_day) = 2 then 'Tuesday'
        when extract(dayofweek from date_day) = 3 then 'Wednesday'
        when extract(dayofweek from date_day) = 4 then 'Thursday'
        when extract(dayofweek from date_day) = 5 then 'Friday'
        when extract(dayofweek from date_day) = 6 then 'Saturday'
    end as day_name,
    case 
        when extract(dayofweek from date_day) in (0, 6) then 'Weekend' 
        else 'Weekday'
    end as weekday_indicator
from date_spine
)
select * from final
order by date_key