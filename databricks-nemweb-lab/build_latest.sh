#!/bin/bash
# Build wheel and create latest.whl + environment.yml for base environment

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="$SCRIPT_DIR/src"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"

echo "Building wheel..."
cd "$SRC_DIR"
uv build --wheel --out-dir dist

# Find the built wheel
WHEEL=$(ls -t dist/nemweb_datasource-*.whl 2>/dev/null | head -1)
if [ -z "$WHEEL" ]; then
    echo "ERROR: No wheel found in dist/"
    exit 1
fi

echo "Built: $WHEEL"

# Copy to artifacts (keeping actual version name)
mkdir -p "$ARTIFACTS_DIR"
# Clean old wheels first
rm -f "$ARTIFACTS_DIR"/nemweb_datasource-*.whl
cp "$WHEEL" "$ARTIFACTS_DIR/"
echo "Copied to: $ARTIFACTS_DIR/$(basename $WHEEL)"

# Extract wheel filename for environment.yml
WHEEL_NAME=$(basename "$WHEEL")

# Create environment.yml for base environment
# Note: Path needs to be updated to match your deployment location
cat > "$ARTIFACTS_DIR/environment.yml" << EOF
# Base environment for NEMWEB Lab
# Deploy this via: Settings > Compute > Base environments > Manage
environment_version: '4'
dependencies:
  - /Workspace/Users/<your-email>/.bundle/nemweb-lab/dev/files/databricks-nemweb-lab/artifacts/$WHEEL_NAME
EOF

echo "Created: $ARTIFACTS_DIR/environment.yml"
echo ""
echo "Build complete!"
echo ""
echo "Next steps:"
echo "  Option 1 (Recommended): Use Databricks Asset Bundles"
echo "    databricks bundle deploy --var='environment=dev'"
echo ""
echo "  Option 2: Manual deployment (legacy)"
echo "    ./deploy_wheel.sh [catalog] [schema]"
