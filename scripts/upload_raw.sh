#!/usr/bin/env bash
# Upload the zone lookup first (no trigger), then the trip file (triggers the pipeline).
# Usage: BUCKET=<data-bucket> ./scripts/upload_raw.sh data/taxi_zone_lookup.csv data/yellow_tripdata_2025-01.parquet
set -euo pipefail
: "${BUCKET:?set BUCKET}"
LOOKUP="${1:?path to taxi_zone_lookup.csv}"
TRIPS="${2:?path to yellow_tripdata_YYYY-MM.parquet}"

aws s3 cp "$LOOKUP" "s3://${BUCKET}/raw/taxi_zone_lookup/"
aws s3 cp "$TRIPS"  "s3://${BUCKET}/raw/yellow_tripdata/"
