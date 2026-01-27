#!/bin/bash
# Deploy wheel to Databricks UC Volume
# Usage: ./deploy_wheel.sh [catalog] [schema]

set -e

CATALOG="${1:-workspace}"
SCHEMA="${2:-nemweb_lab}"
VERSION="2.10.0"
WHEEL_NAME="nemweb_datasource-${VERSION}-py3-none-any.whl"
VOLUME_PATH="/Volumes/${CATALOG}/${SCHEMA}/artifacts"

# Build wheel
echo "Building wheel..."
cd "$(dirname "$0")/src"
uv build --wheel --out-dir ../dist

# Deploy to volume
echo "Deploying to ${VOLUME_PATH}..."
databricks fs cp "../dist/${WHEEL_NAME}" "dbfs:${VOLUME_PATH}/${WHEEL_NAME}" --overwrite

# Generate environment.yml
ENV_CONTENT="environment_version: '4'
dependencies:
  - ${VOLUME_PATH}/${WHEEL_NAME}"

echo "Updating environment.yml..."
echo "${ENV_CONTENT}" | databricks fs cp - "dbfs:${VOLUME_PATH}/environment.yml" --overwrite

echo ""
echo "Deployed successfully!"
echo "  Wheel: ${VOLUME_PATH}/${WHEEL_NAME}"
echo "  Env:   ${VOLUME_PATH}/environment.yml"
echo ""
echo "Configure your base environment:"
echo "  1. Workspace Settings > Compute > Environment management"
echo "  2. Add: ${VOLUME_PATH}/environment.yml"
