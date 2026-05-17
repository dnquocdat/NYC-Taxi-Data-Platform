# dbt Seeds

`payment_type.csv`, `vendor.csv`, and `rate_code.csv` are small TLC code mappings and are safe to keep in the repository.

`taxi_zone_lookup.csv` is intentionally committed with only the official header so the project does not ship fake production geography. Before running production dbt models, replace it with the official NYC TLC Taxi Zone Lookup file:

```bash
curl -L "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv" -o dbt/nyc_taxi/seeds/taxi_zone_lookup.csv
```

Then run:

```bash
make dbt-seed
```
