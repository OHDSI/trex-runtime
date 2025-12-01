#!/bin/bash
set -e

echo "======================================"
echo "V8 Source Build Setup"
echo "======================================"
echo ""

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ]; then
    echo "Error: Please run this script from the edge-runtime root directory"
    exit 1
fi

echo "Step 1: Initialize V8 submodules (this may take a while)..."
cd thridparty/supabase/rusty_v8
git submodule update --init --recursive
echo "✓ Submodules initialized"
echo ""

cd ../../..

echo "Step 2: Verify Python installation..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo "✓ Found: $PYTHON_VERSION"
else
    echo "⚠ Python 3 not found. Install with: brew install python@3.11"
    exit 1
fi
echo ""

echo "Step 3: Verify Ninja build tool..."
if command -v ninja &> /dev/null; then
    echo "✓ Ninja found"
else
    echo "⚠ Ninja not found. Install with: brew install ninja"
    exit 1
fi
echo ""

echo "Step 4: Verify Rust toolchain..."
if command -v rustc &> /dev/null; then
    RUST_VERSION=$(rustc --version)
    echo "✓ Found: $RUST_VERSION"
else
    echo "⚠ Rust not found. Install rustup first."
    exit 1
fi
echo ""

echo "======================================"
echo "Setup Complete!"
echo "======================================"
echo ""
echo "Now build V8 from source with:"
echo ""
echo "  export V8_FROM_SOURCE=1"
echo "  export RUSTY_V8_MIRROR=https://github.com/denoland/rusty_v8/releases/download"
echo "  export PATH=\"\$HOME/.rustup/toolchains/1.90.0-aarch64-apple-darwin/bin:\$PATH\""
echo ""
echo "  # Release build (recommended for macOS stability):"
echo "  cargo build --release"
echo ""
echo "  # Or debug build:"
echo "  cargo build"
echo ""
echo "First build will take 20-40 minutes."
echo "Subsequent builds use cached V8."
echo "Release build is more stable on macOS."
echo ""
