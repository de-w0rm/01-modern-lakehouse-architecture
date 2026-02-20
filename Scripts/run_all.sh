#!/usr/bin/env bash
set -euo pipefail

bash scripts/run_ingest.sh
bash scripts/run_dbt.sh