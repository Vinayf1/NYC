#!/usr/bin/env bash
# Remove everything: pipeline stack first, then empty the bucket, then the resources stack.
# Usage: OWNER=vinay ./scripts/teardown.sh
set -euo pipefail
: "${OWNER:?set OWNER}"
RESOURCES_STACK="taxi-pipeline-${OWNER}-resources"
PIPELINE_STACK="taxi-pipeline-${OWNER}"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
BUCKET="taxi-pipeline-${OWNER}-${ACCOUNT_ID}"   # same name resources.yaml gives the bucket

echo "This will delete stack ${PIPELINE_STACK}, ALL data and artifacts in s3://${BUCKET}, and stack ${RESOURCES_STACK}."
read -r -p "Type 'yes' to continue: " ok
[[ "$ok" == "yes" ]] || { echo "Aborted."; exit 1; }

# 1. Pipeline stack first, so nothing is still writing to the bucket
aws cloudformation delete-stack --stack-name "$PIPELINE_STACK"
aws cloudformation wait stack-delete-complete --stack-name "$PIPELINE_STACK"
echo "Stack ${PIPELINE_STACK} deleted."

# 2. CloudFormation cannot delete a bucket that still contains objects
aws s3 rm "s3://${BUCKET}" --recursive

# 3. Resources stack (the bucket itself)
aws cloudformation delete-stack --stack-name "$RESOURCES_STACK"
aws cloudformation wait stack-delete-complete --stack-name "$RESOURCES_STACK"
echo "Stack ${RESOURCES_STACK} deleted. Nothing left."
