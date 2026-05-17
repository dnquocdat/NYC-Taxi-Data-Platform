select distinct
    pickup_date as date_id,
    toYear(pickup_date) as year,
    toMonth(pickup_date) as month,
    toDayOfMonth(pickup_date) as day_of_month,
    toDayOfWeek(pickup_date) as day_of_week,
    formatDateTime(pickup_date, '%W') as day_name
from {{ ref('stg_yellow_taxi_trips') }}
where pickup_date is not null
