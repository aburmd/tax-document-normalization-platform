#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
LAMBDA_DIR="$REPO_ROOT/lambda/pdf_ingestion"
BUILD_DIR="$REPO_ROOT/.build"
GIT_SHA=$(cd "$REPO_ROOT" && git rev-parse --short HEAD 2>/dev/null || echo "local")
ZIP_NAME="pdf-ingestion-${GIT_SHA}.zip"

echo "=== Packaging Lambda: $ZIP_NAME ==="

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR/package"

pip3 install -r "$LAMBDA_DIR/requirements.txt" -t "$BUILD_DIR/package" --quiet --platform manylinux2014_x86_64 --only-binary=:all:

cp -r "$LAMBDA_DIR"/*.py "$BUILD_DIR/package/"
cp -r "$LAMBDA_DIR/parsers" "$BUILD_DIR/package/"
cp -r "$LAMBDA_DIR/common" "$BUILD_DIR/package/"
cp -r "$REPO_ROOT/schemas" "$BUILD_DIR/package/"

cd "$BUILD_DIR/package"
zip -r "$BUILD_DIR/$ZIP_NAME" . -q

echo "=== Built: .build/$ZIP_NAME ==="
echo "ARTIFACT_KEY=lambda/$ZIP_NAME"
