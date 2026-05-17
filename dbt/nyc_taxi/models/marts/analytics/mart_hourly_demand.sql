select
    pickup_time_id as hour_of_day,
    t.day_part,
    count() as trip_count,
    sum(total_amount) as total_revenue,
    avg(trip_distance) as average_trip_distance
from {{ ref('fact_trips') }} as f
left join {{ ref('dim_time') }} as t
    on f.pickup_time_id = t.time_id
group by pickup_time_id, t.day_part
