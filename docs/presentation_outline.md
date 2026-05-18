# Presentation Outline

Target duration: 20 minutes plus Q&A.

## 1. Problem And Goal - 2 minutes

- State the assignment: build a compact, engineering-grade data platform in one week.
- Emphasize the project goal: completeness, correctness, and explainability over many shallow features.
- Preview the business questions: revenue/trips over time, high-demand zones, payment behavior, and peak demand hours.

## 2. Dataset - 2 minutes

- Introduce NYC TLC Yellow Taxi Trip Records as the main public real-world dataset.
- Introduce Taxi Zone Lookup as the joinable location dimension.
- Explain the dataset threshold: full mode must satisfy at least 20 million records or 10 GiB raw data.
- Clarify that sample mode is only for CI/local smoke tests and is not presented as the production dataset.

## 3. Architecture - 4 minutes

- Walk through the diagram: NYC TLC source to Airflow, Spark, MinIO/Delta, ClickHouse, dbt, and Superset.
- Explain why Spark is used: distributed ingestion and transformations, not pandas.
- Explain why MinIO + Delta Lake: open lakehouse format with local reproducibility.
- Explain why ClickHouse: fast OLAP serving for dbt and Superset.
- Explain why dbt: star schema, tests, and model documentation.

## 4. Pipeline Demo - 3 minutes

- Show or run the Airflow DAG `nyc_taxi_monthly_pipeline`.
- Point out the main tasks: validate dataset, check source, Bronze, Silver, ClickHouse load, dbt seed/run/test.
- Show Bronze metadata columns and partitioning by source year/month.
- Show Silver valid rows and Quarantine invalid rows if available.
- If running live is too slow, run a sample path and explain that full mode is guarded by dataset validation.

## 5. Data Quality - 3 minutes

- Explain Spark rules: timestamps present, dropoff after pickup, positive distance, non-negative fares, required locations.
- Explain deterministic `trip_id` and deduplication.
- Explain Quarantine: invalid records are preserved with reasons, batch ID, and source file.
- Explain dbt tests: unique/not-null trip ID, relationships to dimensions, accepted payment values, and range checks.
- State failure behavior: invalid rows are quarantined, dbt failures block the pipeline, Airflow retries transient failures.

## 6. Dashboard - 2 minutes

- Open Superset or show the dashboard plan from `docs/superset_dashboard.md`.
- Confirm charts query ClickHouse marts, not local files.
- Show planned charts: KPI cards, trips/revenue time series with payment filter, top pickup/dropoff zones, payment summary, hourly heatmap.
- Tie each chart back to one business question.

## 7. Engineering Practices - 2 minutes

- Show repo structure: `src`, `dags`, `dbt`, `tests`, `docs`, `scripts`, Docker Compose, CI.
- Mention code quality: ruff, black, type hints, structured logging, no bare prints.
- Mention tests: unit transformation tests and sample integration test that does not download large data.
- Mention CI: lint, format check, tests, Docker Compose config validation, dbt Docker image build.
- Mention secrets: `.env` ignored, `.env.example` local defaults only.

## 8. Trade-offs And Limitations - 1 minute

- Docker Compose is reproducible and explainable, but not high availability.
- Superset dashboard export is manual until built against a populated running instance.
- Local full runs may be constrained by disk, memory, and network bandwidth.
- The committed taxi zone lookup uses the official seed, so dbt relationship tests can validate location mappings.

## 9. Improvements - 1 minute

- Export/version Superset dashboard assets.
- Add lineage with OpenLineage/Marquez.
- Add Great Expectations or Soda reports.
- Add cloud deployment with Terraform or Helm.
- Add performance benchmarks for larger month ranges.

## Q&A Preparation

- Why partition by month? Source delivery, rerun boundaries, and time-based analysis.
- Why deterministic `trip_id` instead of source ID? NYC TLC records do not provide a stable unique trip ID.
- Why ClickHouse partition replacement? It is deterministic for reruns and easy to explain locally.
- Why not notebooks? The assignment requires production-like pipelines; notebooks can be exploratory but are not the execution path.
- What happens to bad data? It is quarantined with reason metadata, not silently dropped.
