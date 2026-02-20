#!/usr/bin/env bash
set -euo pipefail

python -m ingestion.extract
python -m ingestion.load_bronze