# Presentation Outline

Target duration: 20 minutes plus 15 minutes Q&A.

## 1. Problem

Explain the entrance project goal: compact, complete, engineering-grade data platform.

## 2. Dataset

Introduce NYC TLC Yellow Taxi Trip Records and Taxi Zone Lookup. Explain the threshold validation and why sample data is test-only.

## 3. Architecture

Walk through Airflow, Spark, MinIO Delta Lake, ClickHouse, dbt, and Superset.

## 4. Live Demo

Run or show the pipeline path: ingest, transform, load, dbt tests, dashboard.

## 5. Data Quality

Show validation rules, Quarantine behavior, dbt tests, and failure handling.

## 6. Engineering Practices

Show repository structure, tests, linting/formatting, CI, Docker Compose, and docs.

## 7. Trade-offs

Explain local Docker Compose vs managed cloud, Delta on MinIO, ClickHouse serving, and scope choices.

## 8. Improvements

Discuss what would be improved with more time: lineage, cloud deployment, dashboard export, richer observability.

