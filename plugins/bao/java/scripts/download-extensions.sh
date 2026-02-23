#!/bin/bash
# Download extensions from npm registry and copy to resources/extensions
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RESOURCES_DIR="$PROJECT_DIR/resources/extensions"
TEMP_DIR=$(mktemp -d)

cleanup() {
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

echo "Downloading extensions to $RESOURCES_DIR"
mkdir -p "$RESOURCES_DIR"

# Configure npm for Azure DevOps registry
NPM_REGISTRY="${NPM_REGISTRY:-https://pkgs.dev.azure.com/data2evidence/d2e/_packaging/d2e/npm/registry/}"

cd "$TEMP_DIR"

# Create minimal package.json
cat > package.json << 'EOF'
{
  "name": "extension-downloader",
  "version": "1.0.0",
  "private": true,
  "dependencies": {}
}
EOF

# Create .npmrc for scoped registry
cat > .npmrc << EOF
@trex:registry=$NPM_REGISTRY
EOF

# Download circe extension
echo "Downloading @trex/circe..."
if npm pack @trex/circe 2>/dev/null; then
    tar -xzf trex-circe-*.tgz
    if [ -f package/circe.trex ]; then
        cp package/circe.trex "$RESOURCES_DIR/"
        echo "Installed circe.trex ($(du -h "$RESOURCES_DIR/circe.trex" | cut -f1))"
    else
        echo "Warning: circe.trex not found in package"
        ls -la package/
    fi
    rm -rf package trex-circe-*.tgz
else
    echo "Warning: Could not download @trex/circe - extension will need to be loaded externally"
fi

echo "Done. Extensions in $RESOURCES_DIR:"
ls -la "$RESOURCES_DIR/"
