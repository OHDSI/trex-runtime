#!/bin/bash
# Exit immediately if a command exits with a non-zero status.
set -e

# --- Environment Setup ---
export GRAAL_HOME=/opt/graalvm
export JAVA_HOME=/opt/graalvm
export PATH=/opt/graalvm/bin:$PATH

# --- Build Steps ---
echo "Setting up GraalVM..."
sudo -E bash ./install-graalvm.sh

echo "Installing dependencies..."
sudo apt-get update && sudo apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    bash \
    build-essential \
    cmake \
    make \
    git \
    python3 \
    maven \
    pkg-config \
    libssl-dev \
    xxd \
    unzip \
    zlib1g-dev \
    file

echo "Building extension..."
make configure
make cql2elm-native
make release

echo "Moving extension binary..."
mv build/release/extension/*/*.trex .

echo "Signing extension (if key available)..."
../extension-ci-tools/extension_signing/sign-if-key.sh

echo "Build complete."
