#!/usr/bin/env bash
# PERSONAL-ACCOUNT VERSION: deploy for testing in your own AWS account.
# Differences from the team version: the dependency layer is built from public PyPI
# (no Artifactory needed), simple tags, and optional reserved concurrency.
# (The AWS managed pandas layer can't be used: its pyarrow can't read the zstd-compressed TLC files.)
#
# Usage:
#   OWNER=vinay ALERT_EMAIL=you@example.com \
#   ./scripts/deploy.sh
# Optional: RESERVED_CONCURRENCY=0 if the deploy fails with an "UnreservedConcurrentExecution" error.
# Uses your AWS CLI profile (aws configure / aws sso login); no credentials in this file.
set -euo pipefail
: "${OWNER:?set OWNER}" "${ALERT_EMAIL:?set ALERT_EMAIL}"
RESERVED_CONCURRENCY="${RESERVED_CONCURRENCY:-1}"
GLUE_DATABASE="${GLUE_DATABASE:-taxi_pipeline}"
RESOURCES_STACK="taxi-pipeline-${OWNER}-resources"
PIPELINE_STACK="taxi-pipeline-${OWNER}"
TAGS=("Project=nyc-taxi-pipeline" "Environment=personal-test" "Owner=${OWNER}")
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "Account: $(aws sts get-caller-identity --query Account --output text)  Region: $(aws configure get region || echo "${AWS_REGION:-unset}")"
if command -v cfn-lint >/dev/null; then cfn-lint infra/resources.yaml infra/template.yaml; fi

# 1. Resources stack (data bucket)
aws cloudformation deploy --template-file infra/resources.yaml --stack-name "$RESOURCES_STACK" \
  --no-fail-on-empty-changeset --parameter-overrides "Owner=${OWNER}" --tags "${TAGS[@]}"
BUCKET="$(aws cloudformation describe-stacks --stack-name "$RESOURCES_STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='DataBucketName'].OutputValue" --output text)"
echo "Data bucket: ${BUCKET}"

# 2. Layer and code zips into s3://<bucket>/artifacts/
export BUCKET
LAYER_KEY="$(./scripts/build_layer.sh)"
echo "Using layer s3://${BUCKET}/${LAYER_KEY}"
aws cloudformation package --template-file infra/template.yaml \
  --s3-bucket "$BUCKET" --s3-prefix artifacts/code --output-template-file packaged.yaml

# 3. Pipeline stack
aws cloudformation deploy --template-file packaged.yaml --stack-name "$PIPELINE_STACK" \
  --capabilities CAPABILITY_NAMED_IAM --no-fail-on-empty-changeset \
  --parameter-overrides "Owner=${OWNER}" "AlertEmail=${ALERT_EMAIL}" "DataBucketName=${BUCKET}" \
      "LayerS3Key=${LAYER_KEY}" "ReservedConcurrency=${RESERVED_CONCURRENCY}" \
      "GlueDatabaseName=${GLUE_DATABASE}" \
  --tags "${TAGS[@]}"

aws cloudformation describe-stacks --stack-name "$PIPELINE_STACK" --query "Stacks[0].Outputs" --output table
echo "Next: confirm the SNS subscription email, then run ./scripts/smoke_test.sh"