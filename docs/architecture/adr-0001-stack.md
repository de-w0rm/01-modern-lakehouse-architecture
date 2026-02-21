# ADR-0001: Free modern lakehouse stack (DuckDB + dbt-core + Python + Docker)

## Status
Accepted

## Context
We want a portfolio-grade lakehouse architecture with minimal cost and maximum reproducibility.

## Decision
- DuckDB as local compute engine and query runtime
- Parquet as the storage format for Bronze/Silver/Gold zones
- dbt-core for transformations, tests, and documentation
- Python for ingestion and lightweight orchestration
- Docker + Makefile to standardize developer experience

## Consequences
- Fast local iteration and deterministic builds
- Clear separation of ingestion vs modeling layers
- Easy upgrade path to cloud warehouses/lakehouses if needed