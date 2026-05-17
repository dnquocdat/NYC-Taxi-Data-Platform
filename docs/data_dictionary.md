# Data Dictionary

This document will be expanded as dbt models are implemented.

## `fact_trips`

Grain: one row equals one yellow taxi trip.

Primary key: `trip_id`.

| Column | Type | Description | Source |
| --- | --- | --- | --- |
| `trip_id` | String | Deterministic SHA-256 identifier built from business columns. | Silver transform |
| `pickup_datetime` | DateTime | Trip pickup timestamp. | NYC TLC |
| `dropoff_datetime` | DateTime | Trip dropoff timestamp. | NYC TLC |
| `pickup_location_id` | Integer | Pickup taxi zone ID. | NYC TLC |
| `dropoff_location_id` | Integer | Dropoff taxi zone ID. | NYC TLC |
| `fare_amount` | Decimal | Metered fare amount. | NYC TLC |
| `total_amount` | Decimal | Total charged amount. | NYC TLC |
| `trip_distance` | Float | Trip distance in miles. | NYC TLC |

## Dimensions

- `dim_date`: calendar attributes derived from `pickup_datetime`.
- `dim_time`: hour/minute attributes derived from `pickup_datetime`.
- `dim_location`: taxi zone, borough, service zone.
- `dim_vendor`: TLC vendor code mapping.
- `dim_payment_type`: payment type code mapping.
- `dim_rate_code`: rate code mapping.

## Analytics Marts

- `mart_daily_revenue`: trips and revenue by day.
- `mart_hourly_demand`: demand by pickup hour and date attributes.
- `mart_location_performance`: demand/revenue by pickup and dropoff geography.
- `mart_payment_summary`: revenue and tip behavior by payment type.

