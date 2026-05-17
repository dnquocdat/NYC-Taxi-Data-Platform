select
    f.pickup_date_id as date_id,
    f.payment_type_id,
    p.payment_type_name,
    count() as trip_count,
    sum(f.total_amount) as total_revenue,
    sum(f.fare_amount) as fare_revenue,
    sum(f.tip_amount) as tip_revenue,
    avg(f.fare_amount) as average_fare,
    avg(f.total_amount) as average_total_amount,
    avg(f.trip_distance) as average_trip_distance
from {{ ref('fact_trips') }} as f
left join {{ ref('dim_payment_type') }} as p
    on f.payment_type_id = p.payment_type_id
group by
    f.pickup_date_id,
    f.payment_type_id,
    p.payment_type_name
