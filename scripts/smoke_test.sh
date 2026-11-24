#!/usr/bin/env bash
# End-to-end test: download the data (if needed), upload it, wait for the run, check the manifest,
# and query Athena. Run it twice to test re-runs (counts must be identical).
# Usage: OWNER=vinay ./scripts/smoke_test.sh            (MONTH=2025-01 by default)
set -euo pipefail
: "${OWNER:?set OWNER}"
MONTH="${MONTH:-2025-01}"
GLUE_DATABASE="${GLUE_DATABASE:-taxi_pipeline}"
WAIT_SECONDS="${WAIT_SECONDS:-900}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BUCKET="$(aws cloudformation describe-stacks --stack-name "taxi-pipeline-${OWNER}-resources" \
  --query "Stacks[0].Outputs[?OutputKey=='DataBucketName'].OutputValue" --output text)"
WORKGROUP="taxi-pipeline-${OWNER}"
TRIP_FILE="data/yellow_tripdata_${MONTH}.parquet"
LOOKUP_FILE="data/taxi_zone_lookup.csv"
MANIFEST_KEY="manifests/yellow_tripdata_${MONTH}/_manifest.json"

# 1. Download inputs once
mkdir -p data
[[ -f "$TRIP_FILE" ]]   || curl -fsSL -o "$TRIP_FILE"   "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${MONTH}.parquet"
[[ -f "$LOOKUP_FILE" ]] || curl -fsSL -o "$LOOKUP_FILE" "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# 2. Upload (lookup first; the trip file triggers the pipeline)
STARTED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
BUCKET="$BUCKET" ./scripts/upload_raw.sh "$LOOKUP_FILE" "$TRIP_FILE"
echo "Uploaded at ${STARTED}; waiting for the manifest (up to ${WAIT_SECONDS}s)..."

# 3. Wait for a manifest written after this upload
deadline=$(( $(date +%s) + WAIT_SECONDS ))
while :; do
  modified="$(aws s3api head-object --bucket "$BUCKET" --key "$MANIFEST_KEY" --query LastModified --output text 2>/dev/null || true)"
  if [[ -n "$modified" ]] && python -c "import sys,datetime as d; f=lambda s:d.datetime.fromisoformat(s.replace('Z','+00:00')); sys.exit(0 if f(sys.argv[1])>=f(sys.argv[2]) else 1)" "$modified" "$STARTED"; then
    break
  fi
  if (( $(date +%s) > deadline )); then
    echo "No new manifest after ${WAIT_SECONDS}s. Check: aws logs tail /aws/lambda/taxi-pipeline-transform-${OWNER} --since 30m"
    exit 1
  fi
  sleep 15
done
echo "Manifest:"
aws s3 cp "s3://${BUCKET}/${MANIFEST_KEY}" -

# 4. Athena checks
athena() {
  local qid state
  qid="$(aws athena start-query-execution --work-group "$WORKGROUP" \
    --query-execution-context "Database=${GLUE_DATABASE}" --query-string "$1" \
    --query QueryExecutionId --output text)"
  while :; do
    state="$(aws athena get-query-execution --query-execution-id "$qid" --query QueryExecution.Status.State --output text)"
    [[ "$state" == "SUCCEEDED" ]] && break
    [[ "$state" == "FAILED" || "$state" == "CANCELLED" ]] && { aws athena get-query-execution --query-execution-id "$qid" --query QueryExecution.Status.StateChangeReason --output text; return 1; }
    sleep 2
  done
  aws athena get-query-results --query-execution-id "$qid" --query "ResultSet.Rows[1:]" --output text
}
Y="${MONTH%-*}"; M="${MONTH#*-}"
echo "Curated rows (Athena):     $(athena "SELECT COUNT(*) FROM yellow_trips_curated WHERE year='${Y}' AND month='${M}'")"
echo "Quarantined rows (Athena): $(athena "SELECT COUNT(*) FROM yellow_trips_quarantine WHERE year='${Y}' AND month='${M}'" || echo "0 (no quarantine table yet)")"
echo "Revenue by borough:"
athena "SELECT pickup_borough, ROUND(SUM(total_revenue),2) FROM daily_borough_summary WHERE year='${Y}' AND month='${M}' GROUP BY pickup_borough ORDER BY 2 DESC"
echo "Done. Compare the counts above with the manifest; they must match."
