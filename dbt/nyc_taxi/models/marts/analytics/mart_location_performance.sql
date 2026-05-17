with location_events as (
    select
        'pickup' as location_role,
        pickup_location_id as location_id,
        total_amount,
        trip_distance,
        trip_duration_minutes
    from {{ ref('fact_trips') }}

    union all

    select
        'dropoff' as location_role,
        dropoff_location_id as location_id,
        total_amount,
        trip_distance,
        trip_duration_minutes
    from {{ ref('fact_trips') }}
)

select
    e.location_role,
    e.location_id,
    l.borough,
    l.zone,
    l.service_zone,
    count() as trip_count,
    sum(e.total_amount) as total_revenue,
    avg(e.trip_distance) as average_trip_distance,
    avg(e.trip_duration_minutes) as average_trip_duration_minutes
from location_events as e
left join {{ ref('dim_location') }} as l
    on e.location_id = l.location_id
group by
    e.location_role,
    e.location_id,
    l.borough,
    l.zone,
    l.service_zone
