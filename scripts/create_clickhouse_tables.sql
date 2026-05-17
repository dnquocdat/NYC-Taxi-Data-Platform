CREATE DATABASE IF NOT EXISTS nyc_taxi;

CREATE TABLE IF NOT EXISTS nyc_taxi.silver_yellow_taxi_trips
(
    trip_id String,
    vendor_id Nullable(UInt16),
    pickup_datetime DateTime,
    dropoff_datetime DateTime,
    passenger_count Nullable(Float64),
    trip_distance Float64,
    rate_code_id Nullable(UInt16),
    store_and_fwd_flag Nullable(String),
    pickup_location_id UInt16,
    dropoff_location_id UInt16,
    payment_type_id Nullable(UInt16),
    fare_amount Float64,
    extra Nullable(Float64),
    mta_tax Nullable(Float64),
    tip_amount Nullable(Float64),
    tolls_amount Nullable(Float64),
    improvement_surcharge Nullable(Float64),
    total_amount Float64,
    congestion_surcharge Nullable(Float64),
    airport_fee Nullable(Float64),
    source_file String,
    source_url String,
    ingestion_timestamp DateTime,
    batch_id String,
    pickup_date Date,
    pickup_hour UInt8,
    trip_duration_minutes Float64,
    average_speed_mph Nullable(Float64),
    pickup_year UInt16,
    pickup_month UInt8
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(pickup_datetime)
ORDER BY (pickup_date, pickup_location_id, dropoff_location_id, trip_id);
