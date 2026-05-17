select
    f.pickup_location_id as location_id,
    l.borough,
    l.zone,
    l.service_zone,
    count() as pickup_trip_count,
    sum(f.total_amount) as pickup_revenue,
    avg(f.trip_distance) as average_trip_distance,
    avg(f.trip_duration_minutes) as average_trip_duration_minutes
from {{ ref('fact_trips') }} as f
left join {{ ref('dim_location') }} as l
    on f.pickup_location_id = l.location_id
group by
    f.pickup_location_id,
    l.borough,
    l.zone,
    l.service_zone
