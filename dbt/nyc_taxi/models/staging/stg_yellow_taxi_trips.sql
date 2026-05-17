with source as (
    select * from {{ source('serving', 'silver_yellow_taxi_trips') }}
)

select
    trip_id,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_fwd_flag,
    pickup_location_id,
    dropoff_location_id,
    payment_type_id,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    source_file,
    source_url,
    ingestion_timestamp,
    batch_id,
    pickup_date,
    pickup_hour,
    trip_duration_minutes,
    average_speed_mph,
    pickup_year,
    pickup_month
from source
