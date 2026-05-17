.PHONY: lint format test docker-up docker-down docker-logs pipeline-sample dbt-run dbt-test

PYTHON ?= python
DBT_DIR ?= dbt/nyc_taxi

lint:
	$(PYTHON) -m ruff check src tests dags scripts
	$(PYTHON) -m black --check src tests dags scripts

format:
	$(PYTHON) -m ruff check --fix src tests dags scripts
	$(PYTHON) -m black src tests dags scripts

test:
	$(PYTHON) -m pytest

docker-up:
	docker compose --env-file .env up -d

docker-down:
	docker compose --env-file .env down

docker-logs:
	docker compose --env-file .env logs -f --tail=200

pipeline-sample:
	$(PYTHON) -m nyc_taxi_pipeline.cli run-sample

dbt-run:
	dbt run --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

dbt-test:
	dbt test --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)
