select
    f.pickup_time_id as hour_of_day,
    t.day_part,
    d.day_of_week,
    d.day_name,
    count() as trip_count,
    sum(f.total_amount) as total_revenue,
    avg(f.trip_distance) as average_trip_distance
from {{ ref('fact_trips') }} as f
left join {{ ref('dim_time') }} as t
    on f.pickup_time_id = t.time_id
left join {{ ref('dim_date') }} as d
    on f.pickup_date_id = d.date_id
group by
    f.pickup_time_id,
    t.day_part,
    d.day_of_week,
    d.day_name
