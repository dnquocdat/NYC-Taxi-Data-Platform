select
    pickup_date_id as date_id,
    count() as trip_count,
    sum(total_amount) as total_revenue,
    sum(fare_amount) as fare_revenue,
    sum(tip_amount) as tip_revenue,
    avg(fare_amount) as average_fare,
    avg(total_amount) as average_total_amount,
    avg(trip_distance) as average_trip_distance
from {{ ref('fact_trips') }}
group by pickup_date_id
