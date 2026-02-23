#!/bin/bash
# Exit immediately if a command exits with a non-zero status.
set -e
export DUCKDB_VERSION=v1.3.2

sudo apt-get update
sudo apt-get install -y build-essential cmake python3 python3-pip pkg-config libssl-dev libvulkan1 libvulkan-dev vulkan-tools glslc libshaderc1 spirv-tools vulkan-validationlayers mesa-vulkan-drivers 

export DUCKDB_VERSION=v1.3.2
echo "Building extension..."
make configure
make

echo "Moving extension binary..."
mv build/release/extension/*/*.trex .

echo "Signing extension (if key available)..."
../extension-ci-tools/extension_signing/sign-if-key.sh

echo "Build complete."
