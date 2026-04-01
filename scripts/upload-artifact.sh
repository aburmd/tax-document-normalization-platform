#!/bin/bash
set -e

ENV="${1:-dev}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$REPO_ROOT/.build"
GIT_SHA=$(cd "$REPO_ROOT" && git rev-parse --short HEAD 2>/dev/null || echo "local")
ZIP_NAME="pdf-ingestion-${GIT_SHA}.zip"
BUCKET="tax-doc-artifacts-${ENV}"
S3_KEY="lambda/${ZIP_NAME}"

if [ ! -f "$BUILD_DIR/$ZIP_NAME" ]; then
  echo "ERROR: $BUILD_DIR/$ZIP_NAME not found. Run ./scripts/package.sh first."
  exit 1
fi

echo "=== Uploading to s3://${BUCKET}/${S3_KEY} ==="
aws s3 cp "$BUILD_DIR/$ZIP_NAME" "s3://${BUCKET}/${S3_KEY}"

echo "=== Done ==="
echo "Deploy with:"
echo "  cd ~/gitworkspace/tax-document-normalization-platform-cdk/cdk"
echo "  cdk deploy TaxDocIngestionStack-${ENV} -c env=${ENV} -c lambdaArtifactKey=${S3_KEY}"
