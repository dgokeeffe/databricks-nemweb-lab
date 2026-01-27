#!/bin/bash
# Deploy wheel to Databricks UC Volume
# 
# This is a LEGACY script for manual deployment. For modern deployments,
# use Databricks Asset Bundles instead:
#   databricks bundle deploy --var="environment=dev"
#
# Usage: ./deploy_wheel.sh [catalog] [schema]

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="$SCRIPT_DIR/src"

CATALOG="${1:-workspace}"
SCHEMA="${2:-nemweb_lab}"
VOLUME_PATH="/Volumes/${CATALOG}/${SCHEMA}/artifacts"

# Extract version from pyproject.toml
# Try Python first (more reliable), fall back to grep+sed
VERSION=$(python3 << PYEOF
import re
with open('$SRC_DIR/pyproject.toml', 'r') as f:
    content = f.read()
    match = re.search(r'^version\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
    if match:
        print(match.group(1))
PYEOF
)
if [ -z "$VERSION" ]; then
    # Fallback: simple grep+sed
    VERSION=$(grep -E '^version\s*=' "$SRC_DIR/pyproject.toml" | head -1 | sed -E 's/.*version\s*=\s*["'\'']([^"'\'']+)["'\''].*/\1/')
fi
if [ -z "$VERSION" ]; then
    echo "ERROR: Could not extract version from pyproject.toml"
    echo "Make sure version is defined in [project] section as: version = \"X.Y.Z\""
    exit 1
fi

WHEEL_NAME="nemweb_datasource-${VERSION}-py3-none-any.whl"
echo "Deploying version: $VERSION"

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
echo "NOTE: This is a legacy manual deployment method."
echo "For modern deployments, use Databricks Asset Bundles:"
echo "  databricks bundle deploy --var='environment=dev'"
echo ""
echo "To configure as base environment manually:"
echo "  1. Workspace Settings > Compute > Environment management"
echo "  2. Add: ${VOLUME_PATH}/environment.yml"
