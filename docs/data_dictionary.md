# Data Dictionary

This dictionary covers the dbt serving layer built in ClickHouse.

## `fact_trips`

Grain: one row equals one yellow taxi trip.

Primary key: `trip_id`.

| Column | Type | Description | Source |
| --- | --- | --- | --- |
| `trip_id` | String | Deterministic SHA-256 identifier built from business columns. | Silver transform |
| `pickup_datetime` | DateTime | Trip pickup timestamp. | NYC TLC |
| `dropoff_datetime` | DateTime | Trip dropoff timestamp. | NYC TLC |
| `pickup_date_id` | Date | Foreign key to `dim_date.date_id`. | Derived from pickup timestamp |
| `pickup_time_id` | UInt8 | Foreign key to `dim_time.time_id`. | Derived from pickup timestamp |
| `vendor_id` | UInt16 | Foreign key to `dim_vendor.vendor_id`. | NYC TLC |
| `rate_code_id` | UInt16 | Foreign key to `dim_rate_code.rate_code_id`. | NYC TLC |
| `payment_type_id` | UInt16 | Foreign key to `dim_payment_type.payment_type_id`. | NYC TLC |
| `pickup_location_id` | Integer | Pickup taxi zone ID. | NYC TLC |
| `dropoff_location_id` | Integer | Dropoff taxi zone ID. | NYC TLC |
| `passenger_count` | Float | Passenger count reported by vendor. | NYC TLC |
| `trip_duration_minutes` | Float | Trip duration in minutes. | Silver transform |
| `average_speed_mph` | Float | Derived average trip speed. | Silver transform |
| `fare_amount` | Decimal | Metered fare amount. | NYC TLC |
| `tip_amount` | Decimal | Tip amount. | NYC TLC |
| `total_amount` | Decimal | Total charged amount. | NYC TLC |
| `trip_distance` | Float | Trip distance in miles. | NYC TLC |
| `source_file` | String | Source parquet file name. | Bronze metadata |
| `source_url` | String | Source parquet URL. | Bronze metadata |
| `ingestion_timestamp` | DateTime | Bronze ingestion timestamp. | Bronze metadata |
| `batch_id` | String | Pipeline batch identifier. | Runtime metadata |

## `dim_date`

Primary key: `date_id`.

| Column | Type | Description | Source |
| --- | --- | --- | --- |
| `date_id` | Date | Calendar date. | fact pickup date |
| `year` | UInt16 | Calendar year. | Derived |
| `month` | UInt8 | Calendar month. | Derived |
| `day_of_month` | UInt8 | Calendar day of month. | Derived |
| `day_of_week` | UInt8 | ClickHouse day-of-week number. | Derived |
| `day_name` | String | Weekday display name. | Derived |

## `dim_time`

Primary key: `time_id`.

| Column | Type | Description | Source |
| --- | --- | --- | --- |
| `time_id` | UInt8 | Hour of day, 0-23. | Generated |
| `hour_of_day` | UInt8 | Hour of day, 0-23. | Generated |
| `day_part` | String | Morning/afternoon/evening/night bucket. | Generated |

## `dim_location`

Primary key: `location_id`.

| Column | Type | Description | Source |
| --- | --- | --- | --- |
| `location_id` | UInt16 | TLC taxi zone ID. | NYC TLC Taxi Zone Lookup |
| `borough` | String | Borough name. | NYC TLC Taxi Zone Lookup |
| `zone` | String | Taxi zone name. | NYC TLC Taxi Zone Lookup |
| `service_zone` | String | TLC service zone. | NYC TLC Taxi Zone Lookup |

## Code Dimensions

| Table | Primary Key | Description |
| --- | --- | --- |
| `dim_vendor` | `vendor_id` | TLC yellow taxi vendor code mapping. |
| `dim_payment_type` | `payment_type_id` | TLC payment type code mapping. |
| `dim_rate_code` | `rate_code_id` | TLC rate code mapping. |

## Analytics Marts

| Mart | Grain | Business Question |
| --- | --- | --- |
| `mart_daily_revenue` | One row per pickup date and payment type | How do trips and revenue change over time and by payment type? |
| `mart_hourly_demand` | One row per pickup hour and weekday | Which pickup hours and weekdays have the most demand? |
| `mart_location_performance` | One row per pickup/dropoff role and location | Which pickup/dropoff zones produce the most trips and revenue? |
| `mart_payment_summary` | One row per payment type | How does payment type affect tips and revenue? |
