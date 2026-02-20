#!/usr/bin/env bash
set -euo pipefail

cd dbt
dbt deps
dbt run
dbt test