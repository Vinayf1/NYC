#!/usr/bin/env bash

# PERSONAL VERSION:
# Build the Lambda dependency layer from public PyPI
# (or Artifactory if credentials are provided)
# and upload it to the data bucket under artifacts/layers/.
#
# Why not the AWS-managed pandas layer:
# Its PyArrow build does NOT include ZSTD support, while the
# NYC TLC source Parquet files can use ZSTD compression.
#
# This build downloads Linux x86_64 wheels compatible with:
#   Lambda runtime: Python 3.12
#   Architecture:   x86_64
#
# The local Python installation is only used to run pip.
# It does NOT have to be Python 3.12 because pip is explicitly
# instructed to download Python 3.12 Linux wheels.
#
# Usage:
#   BUCKET=<data-bucket> ./scripts/build_layer.sh
#
# Optional:
#   ARTIFACTORY_PYPI_URL=<Artifactory PyPI index>
#   ARTIFACTORY_USERNAME=<username>
#   ARTIFACTORY_PASSWORD=<token/password>
#   FORCE_BUILD=1
#
# Never run this script with set -x because credentials
# could be exposed in terminal/CI logs.

set -euo pipefail

: "${BUCKET:?set BUCKET}"

ARTIFACTORY_PYPI_URL="${ARTIFACTORY_PYPI_URL:-artifactory-uw2.adobeitc.com/artifactory/api/pypi/pypi-adobe-acs-release/simple}"

PYTHON_VERSION="3.12"
PLATFORM="manylinux2014_x86_64"

# Leave some space under Lambda's combined unzipped package/layer limit.
MAX_UNZIPPED_MB=240

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

REQUIREMENTS="$ROOT/layer/requirements.txt"
BUILD_DIR="$ROOT/build/layer"
SITE_PACKAGES="$BUILD_DIR/python/lib/python${PYTHON_VERSION}/site-packages"
ZIP_FILE="$ROOT/build/layer.zip"

log() {
  echo "[build_layer] $*" >&2
}

# ------------------------------------------------------------
# Validate prerequisites
# ------------------------------------------------------------

if ! command -v python >/dev/null 2>&1; then
  log "ERROR: Python was not found."
  exit 1
fi

if ! python -m pip --version >/dev/null 2>&1; then
  log "ERROR: pip is not available for the current Python installation."
  exit 1
fi

if [[ ! -f "$REQUIREMENTS" ]]; then
  log "ERROR: requirements file not found: $REQUIREMENTS"
  exit 1
fi

# ------------------------------------------------------------
# Generate deterministic S3 key
# ------------------------------------------------------------

HASH="$(
  cat "$REQUIREMENTS" "${BASH_SOURCE[0]}" |
    sha256sum |
    cut -c1-12
)"

LAYER_KEY="artifacts/layers/taxi-pipeline-deps-py${PYTHON_VERSION}-${HASH}.zip"

# ------------------------------------------------------------
# Skip build if artifact already exists
# ------------------------------------------------------------

if [[ "${FORCE_BUILD:-0}" != "1" ]] &&
   aws s3api head-object \
     --bucket "$BUCKET" \
     --key "$LAYER_KEY" \
     >/dev/null 2>&1; then

  log "Layer already exists at s3://${BUCKET}/${LAYER_KEY}; skipping build."

  echo "$LAYER_KEY"
  exit 0
fi

# ------------------------------------------------------------
# Prepare build directory
# ------------------------------------------------------------

log "Building layer for Lambda Python ${PYTHON_VERSION} (${PLATFORM})..."

rm -rf "$BUILD_DIR" "$ZIP_FILE"

mkdir -p "$SITE_PACKAGES"

# ------------------------------------------------------------
# Configure package repository
# ------------------------------------------------------------

if [[ -n "${ARTIFACTORY_USERNAME:-}" &&
      -n "${ARTIFACTORY_PASSWORD:-}" ]]; then

  log "Installing dependencies from Artifactory"

  ENC_USER="$(
    python -c \
      'import os, urllib.parse; print(urllib.parse.quote(os.environ["ARTIFACTORY_USERNAME"], safe=""))'
  )"

  ENC_PASS="$(
    python -c \
      'import os, urllib.parse; print(urllib.parse.quote(os.environ["ARTIFACTORY_PASSWORD"], safe=""))'
  )"

  export PIP_INDEX_URL="https://${ENC_USER}:${ENC_PASS}@${ARTIFACTORY_PYPI_URL}"

else

  log "Installing dependencies from public PyPI"

fi

# ------------------------------------------------------------
# Download/install Linux Python 3.12 wheels
#
# Important:
# Even though the local machine is Windows and currently has
# Python 3.14, pip downloads packages specifically for:
#
#   Python 3.12
#   CPython
#   Linux x86_64
#
# This matches the Lambda runtime.
# ------------------------------------------------------------

python -m pip install \
  --quiet \
  --disable-pip-version-check \
  --requirement "$REQUIREMENTS" \
  --target "$SITE_PACKAGES" \
  --platform "$PLATFORM" \
  --implementation cp \
  --python-version "$PYTHON_VERSION" \
  --only-binary=:all: \
  --upgrade

# Remove credentials from environment after pip completes.

unset PIP_INDEX_URL ENC_USER ENC_PASS 2>/dev/null || true

# ------------------------------------------------------------
# Remove unnecessary files
# ------------------------------------------------------------

log "Trimming files Lambda does not need..."

pushd "$SITE_PACKAGES" >/dev/null

# boto3/botocore/s3transfer already exist in the Lambda
# Python runtime, so don't duplicate them in the layer.

rm -rf \
  boto3 \
  boto3-*.dist-info \
  botocore \
  botocore-*.dist-info \
  s3transfer \
  s3transfer-*.dist-info

# Remove test directories.

for pkg in pandas numpy pyarrow; do
  if [[ -d "$pkg" ]]; then
    find "$pkg" \
      -type d \
      -name tests \
      -prune \
      -exec rm -rf {} +
  fi
done

# Remove Python bytecode caches.

find . \
  -type d \
  -name "__pycache__" \
  -prune \
  -exec rm -rf {} +

# Headers and Cython sources aren't required at Lambda runtime.

rm -rf \
  pyarrow/include \
  pyarrow/src \
  numpy/_core/include

find pyarrow \
  \( -name "*.pyx" -o -name "*.pxd" -o -name "*.pxi" \) \
  -type f \
  -delete 2>/dev/null || true

popd >/dev/null

# ------------------------------------------------------------
# Check layer size
# ------------------------------------------------------------

SIZE_MB="$(du -sm "$BUILD_DIR" | cut -f1)"

log "Unzipped layer size: ${SIZE_MB} MB"
log "Configured layer limit: ${MAX_UNZIPPED_MB} MB"

if (( SIZE_MB > MAX_UNZIPPED_MB )); then

  log "ERROR: Layer is too large for the Lambda ZIP/layer deployment."

  log "Do NOT remove native PyArrow/Numpy .so libraries."

  log "Consider pinning compatible smaller dependency versions"
  log "or switching the Lambda deployment to a container image."

  exit 1
fi

# ------------------------------------------------------------
# Create ZIP
# ------------------------------------------------------------

log "Creating Lambda layer ZIP..."

rm -f "$ZIP_FILE"

(
  cd "$BUILD_DIR"
  python -m zipfile -c "$ZIP_FILE" python
)

# ------------------------------------------------------------
# Upload to S3
# ------------------------------------------------------------

log "Uploading layer to:"
log "s3://${BUCKET}/${LAYER_KEY}"

aws s3 cp \
  "$ZIP_FILE" \
  "s3://${BUCKET}/${LAYER_KEY}" \
  --only-show-errors \
  >&2

log "Layer upload completed."

# IMPORTANT:
# Only the S3 key is printed to stdout because deploy.sh
# captures this value.

echo "$LAYER_KEY"