# Superset Dashboard Setup

This guide prepares the BI layer for the NYC Taxi platform. Superset connects directly to ClickHouse and all charts should query dbt marts, not local CSV extracts or cached files.

## Prerequisites

Run the upstream serving path first:

```bash
make docker-up
make create-clickhouse-tables
python -m nyc_taxi_pipeline.cli load-clickhouse
make dbt-seed
make dbt-run
make dbt-test
```

The committed `dbt/nyc_taxi/seeds/taxi_zone_lookup.csv` contains the official NYC TLC lookup data used by location charts and dbt relationship tests.

## ClickHouse Driver

The local Superset service uses [superset/Dockerfile](../superset/Dockerfile), which extends the Apache Superset image and installs `clickhouse-connect`. That driver provides the `clickhousedb://` SQLAlchemy URI used below.

Rebuild Superset after changing the Dockerfile:

```bash
docker compose --env-file .env build superset
docker compose --env-file .env up -d superset
```

## Connect Superset To ClickHouse

1. Open Superset: `http://localhost:8088`.
2. Login with `SUPERSET_ADMIN_USER` and `SUPERSET_ADMIN_PASSWORD` from `.env`.
3. Go to **Settings > Database Connections > + Database**.
4. Choose **Other** if ClickHouse is not listed.
5. Use a SQLAlchemy URI generated from `.env`:

```bash
python scripts/superset_clickhouse_uri.py --show-password
```

For the default local `.env`, the URI shape is:

```text
clickhousedb://<CLICKHOUSE_USER>:<CLICKHOUSE_PASSWORD>@clickhouse:8123/nyc_taxi
```

Use host `clickhouse`, not `localhost`, because Superset runs inside the Docker Compose network. Test the connection and save it as `ClickHouse NYC Taxi`.

## Datasets

Create Superset datasets from these ClickHouse dbt marts:

| Superset Dataset | ClickHouse Table | Purpose |
| --- | --- | --- |
| `mart_daily_revenue` | `nyc_taxi.mart_daily_revenue` | Date trend, KPI totals, payment-type time filters |
| `mart_hourly_demand` | `nyc_taxi.mart_hourly_demand` | Hour/day demand heatmap |
| `mart_location_performance` | `nyc_taxi.mart_location_performance` | Pickup and dropoff zone ranking |
| `mart_payment_summary` | `nyc_taxi.mart_payment_summary` | Payment type revenue and tip behavior |

All charts below should use these datasets. Do not upload local files into Superset for the demo dashboard.

## Dashboard

Create a dashboard named `NYC Taxi Operations`.

### KPI Cards

Use `mart_daily_revenue`.

| Chart | Metric | Notes |
| --- | --- | --- |
| Total Trips | `SUM(trip_count)` | Big number |
| Total Revenue | `SUM(total_revenue)` | Currency formatting |
| Average Fare | `AVG(average_fare)` | Currency formatting |
| Average Trip Distance | `AVG(average_trip_distance)` | Miles |

### Time Series: Trips And Revenue Over Time

Dataset: `mart_daily_revenue`

- X-axis: `date_id`
- Metrics: `SUM(trip_count)`, `SUM(total_revenue)`
- Visualization: mixed time-series line chart, or two coordinated time-series charts
- Dashboard filter/drill-down dimension: `payment_type_name`

Business question answered: How do revenue and trip counts change by day/month, and how does payment type affect the trend?

### Bar Chart: Top Pickup And Dropoff Zones

Dataset: `mart_location_performance`

- Dimension: `zone`
- Filter: `location_role` equals `pickup` for pickup demand, or `dropoff` for dropoff demand
- Metric: `SUM(trip_count)`
- Sort: descending by `SUM(trip_count)`
- Limit: 10 or 20

Business question answered: Which pickup and dropoff zones have the highest demand?

### Bar Or Pie Chart: Revenue By Payment Type

Dataset: `mart_payment_summary`

- Dimension: `payment_type_name`
- Metric: `SUM(total_revenue)`
- Optional metrics: `SUM(total_tips)`, `AVG(average_tip_amount)`, `AVG(tip_revenue_ratio)`

Business question answered: How does payment type affect revenue and tips?

### Heatmap: Demand By Pickup Hour And Weekday

Dataset: `mart_hourly_demand`

- X-axis: `hour_of_day`
- Y-axis: `day_name` or `day_of_week`
- Metric: `SUM(trip_count)`
- Visualization: heatmap

Business question answered: Which hours and weekdays have the highest demand?

## Suggested Layout

1. First row: four KPI cards.
2. Second row: trips/revenue time series with a `payment_type_name` native filter.
3. Third row: top pickup zones and top dropoff zones side-by-side.
4. Fourth row: revenue by payment type and hourly demand heatmap.

## Native Filters

Add dashboard filters:

- `date_id` time range from `mart_daily_revenue`.
- `payment_type_name` from `mart_daily_revenue`.
- `location_role` from `mart_location_performance`, defaulting to `pickup` for zone charts.

## Export

After building the dashboard manually:

1. Open the dashboard.
2. Use **Export** from the dashboard actions menu.
3. Save the ZIP or JSON artifacts under `superset/dashboard_export/`.

No generated dashboard export is committed yet because the dashboard requires a running Superset instance with a populated ClickHouse database and generated internal IDs.
