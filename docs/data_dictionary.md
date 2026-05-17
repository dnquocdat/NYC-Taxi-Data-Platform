# Data Dictionary

This dictionary covers the dbt serving layer built in ClickHouse. Types are the intended analytical types from dbt/ClickHouse models. Nullable is based on model intent; source system optionality can still produce nulls before tests fail.

## `fact_trips`

Grain: one row equals one validated yellow taxi trip.

Primary key: `trip_id`.

| Column | Type | Nullable | Description | Source |
| --- | --- | --- | --- | --- |
| `trip_id` | String | No | Deterministic SHA-256 identifier built from business columns. | Silver transform |
| `pickup_datetime` | DateTime | No | Trip pickup timestamp. | NYC TLC |
| `dropoff_datetime` | DateTime | No | Trip dropoff timestamp. | NYC TLC |
| `pickup_date_id` | Date | No | Foreign key to `dim_date.date_id`. | Derived from pickup timestamp |
| `pickup_time_id` | UInt8 | No | Foreign key to `dim_time.time_id`. | Derived from pickup timestamp |
| `vendor_id` | UInt16 | Yes | Foreign key to `dim_vendor.vendor_id`. | NYC TLC |
| `rate_code_id` | UInt16 | Yes | Foreign key to `dim_rate_code.rate_code_id`. | NYC TLC |
| `payment_type_id` | UInt16 | Yes | Foreign key to `dim_payment_type.payment_type_id`. | NYC TLC |
| `pickup_location_id` | UInt16 | No | Pickup taxi zone ID. | NYC TLC |
| `dropoff_location_id` | UInt16 | No | Dropoff taxi zone ID. | NYC TLC |
| `passenger_count` | Float64 | Yes | Passenger count reported by the vendor. | NYC TLC |
| `trip_duration_minutes` | Float64 | No | Trip duration in minutes. | Silver transform |
| `average_speed_mph` | Float64 | Yes | Average trip speed in miles per hour; null when duration is zero or invalid before filtering. | Silver transform |
| `fare_amount` | Decimal/Float | No | Metered fare amount in USD. | NYC TLC |
| `tip_amount` | Decimal/Float | Yes | Tip amount in USD. | NYC TLC |
| `total_amount` | Decimal/Float | No | Total charged amount in USD. | NYC TLC |
| `trip_distance` | Float64 | No | Trip distance in miles. | NYC TLC |
| `source_file` | String | No | Source Parquet file name. | Bronze metadata |
| `source_url` | String | No | Source Parquet URL. | Bronze metadata |
| `ingestion_timestamp` | DateTime | No | Bronze ingestion timestamp. | Bronze metadata |
| `batch_id` | String | No | Pipeline batch identifier. | Runtime metadata |

## `dim_date`

Primary key: `date_id`.

| Column | Type | Nullable | Description | Source |
| --- | --- | --- | --- | --- |
| `date_id` | Date | No | Calendar date used for pickup-date joins. | `fact_trips.pickup_date_id` |
| `year` | UInt16 | No | Calendar year. | Derived |
| `month` | UInt8 | No | Calendar month number. | Derived |
| `day_of_month` | UInt8 | No | Day of month. | Derived |
| `day_of_week` | UInt8 | No | ClickHouse day-of-week number. | Derived |
| `day_name` | String | No | Weekday display name. | Derived |

## `dim_time`

Primary key: `time_id`.

| Column | Type | Nullable | Description | Source |
| --- | --- | --- | --- | --- |
| `time_id` | UInt8 | No | Hour of day, 0 through 23. | Generated from `numbers(24)` |
| `hour_of_day` | UInt8 | No | Hour of day, 0 through 23. | Generated |
| `day_part` | String | No | Coarse bucket: night, morning, afternoon, or evening. | Generated |

## `dim_location`

Primary key: `location_id`.

| Column | Type | Nullable | Description | Source |
| --- | --- | --- | --- | --- |
| `location_id` | UInt16 | No | TLC taxi zone ID. | NYC TLC Taxi Zone Lookup |
| `borough` | String | Yes | NYC borough name. | NYC TLC Taxi Zone Lookup |
| `zone` | String | Yes | Taxi zone name. | NYC TLC Taxi Zone Lookup |
| `service_zone` | String | Yes | TLC service zone category. | NYC TLC Taxi Zone Lookup |

## `dim_vendor`

Primary key: `vendor_id`.

| Column | Type | Nullable | Description | Source |
| --- | --- | --- | --- | --- |
| `vendor_id` | UInt16 | No | TLC yellow taxi vendor code. | `vendor.csv` seed |
| `vendor_name` | String | No | Vendor display name. | `vendor.csv` seed |

## `dim_payment_type`

Primary key: `payment_type_id`.

| Column | Type | Nullable | Description | Source |
| --- | --- | --- | --- | --- |
| `payment_type_id` | UInt16 | No | TLC payment type code. | `payment_type.csv` seed |
| `payment_type_name` | String | No | Payment type display name. | `payment_type.csv` seed |

## `dim_rate_code`

Primary key: `rate_code_id`.

| Column | Type | Nullable | Description | Source |
| --- | --- | --- | --- | --- |
| `rate_code_id` | UInt16 | No | TLC rate code. | `rate_code.csv` seed |
| `rate_code_name` | String | No | Rate code display name. | `rate_code.csv` seed |

## `mart_daily_revenue`

Grain: one row per pickup date and payment type.

| Column | Type | Nullable | Description | Source |
| --- | --- | --- | --- | --- |
| `date_id` | Date | No | Pickup date. | `fact_trips.pickup_date_id` |
| `payment_type_id` | UInt16 | Yes | Payment type ID for Superset filtering. | `dim_payment_type` |
| `payment_type_name` | String | Yes | Payment type label for Superset filtering. | `dim_payment_type` |
| `trip_count` | UInt64 | No | Number of trips. | Aggregated fact |
| `total_revenue` | Decimal/Float | No | Sum of `total_amount`. | Aggregated fact |
| `fare_revenue` | Decimal/Float | No | Sum of `fare_amount`. | Aggregated fact |
| `tip_revenue` | Decimal/Float | No | Sum of `tip_amount`. | Aggregated fact |
| `average_fare` | Float64 | Yes | Average fare amount. | Aggregated fact |
| `average_total_amount` | Float64 | Yes | Average total amount. | Aggregated fact |
| `average_trip_distance` | Float64 | Yes | Average trip distance. | Aggregated fact |

## `mart_hourly_demand`

Grain: one row per pickup hour, day part, and weekday.

| Column | Type | Nullable | Description | Source |
| --- | --- | --- | --- | --- |
| `hour_of_day` | UInt8 | No | Pickup hour. | `dim_time` |
| `day_part` | String | No | Coarse day-part bucket. | `dim_time` |
| `day_of_week` | UInt8 | No | Weekday number for heatmaps. | `dim_date` |
| `day_name` | String | No | Weekday label for heatmaps. | `dim_date` |
| `trip_count` | UInt64 | No | Number of trips. | Aggregated fact |
| `total_revenue` | Decimal/Float | No | Sum of total amount. | Aggregated fact |
| `average_trip_distance` | Float64 | Yes | Average trip distance. | Aggregated fact |

## `mart_location_performance`

Grain: one row per location role and taxi zone.

| Column | Type | Nullable | Description | Source |
| --- | --- | --- | --- | --- |
| `location_role` | String | No | `pickup` or `dropoff`. | dbt model |
| `location_id` | UInt16 | No | TLC zone ID. | `fact_trips` |
| `borough` | String | Yes | Borough. | `dim_location` |
| `zone` | String | Yes | Taxi zone name. | `dim_location` |
| `service_zone` | String | Yes | TLC service zone category. | `dim_location` |
| `trip_count` | UInt64 | No | Number of trips for the location role. | Aggregated fact |
| `total_revenue` | Decimal/Float | No | Sum of revenue for trips touching the location role. | Aggregated fact |
| `average_trip_distance` | Float64 | Yes | Average trip distance. | Aggregated fact |
| `average_trip_duration_minutes` | Float64 | Yes | Average trip duration. | Aggregated fact |

## `mart_payment_summary`

Grain: one row per payment type.

| Column | Type | Nullable | Description | Source |
| --- | --- | --- | --- | --- |
| `payment_type_id` | UInt16 | Yes | Payment type ID. | `dim_payment_type` |
| `payment_type_name` | String | Yes | Payment type display name. | `dim_payment_type` |
| `trip_count` | UInt64 | No | Number of trips. | Aggregated fact |
| `total_revenue` | Decimal/Float | No | Revenue by payment type. | Aggregated fact |
| `total_tips` | Decimal/Float | No | Tips by payment type. | Aggregated fact |
| `average_tip_amount` | Float64 | Yes | Average tip amount. | Aggregated fact |
| `average_total_amount` | Float64 | Yes | Average total amount. | Aggregated fact |
| `tip_revenue_ratio` | Float64 | Yes | Tip amount divided by total revenue. | Aggregated fact |
