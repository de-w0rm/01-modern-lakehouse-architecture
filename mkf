SHELL := /bin/bash

.PHONY: help build up down shell setup ingest dbt-run dbt-test full-pipeline clean

help:
	@echo "Targets:"
	@echo "  make build          Build Docker image"
	@echo "  make up             Start container"
	@echo "  make down           Stop container"
	@echo "  make shell          Shell into container"
	@echo "  make setup          Install deps (inside container)"
	@echo "  make ingest         Run ingestion (Bronze)"
	@echo "  make dbt-run        Run dbt models"
	@echo "  make dbt-test       Run dbt tests"
	@echo "  make full-pipeline  Run ingestion + dbt run + dbt test"
	@echo "  make clean          Remove local build artifacts"

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

shell:
	docker compose run --rm lakehouse bash

setup:
	docker compose run --rm lakehouse bash -lc "pip install -r requirements.txt"

ingest:
	docker compose run --rm lakehouse bash -lc "bash scripts/run_ingest.sh"

dbt-run:
	docker compose run --rm lakehouse bash -lc "cd dbt && dbt deps && dbt run"

dbt-test:
	docker compose run --rm lakehouse bash -lc "cd dbt && dbt test"

full-pipeline:
	docker compose run --rm lakehouse bash -lc "bash scripts/run_all.sh"

clean:
	rm -rf dbt/target dbt/logs dbt/dbt_packages

.PHONY: bootstrap
bootstrap:
	docker compose run --rm lakehouse bash -lc "mkdir -p lakehouse lakehouse/bronze lakehouse/silver lakehouse/gold"