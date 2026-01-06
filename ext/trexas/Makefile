.PHONY: clean clean_all

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=trexas

# Set to 1 to enable Unstable API (binaries will only work on TARGET_DUCKDB_VERSION, forwards compatibility will be broken)
# Note: currently extension-template-rs requires this, as duckdb-rs relies on unstable C API functionality
USE_UNSTABLE_C_API=1

# Target DuckDB version
TARGET_DUCKDB_VERSION=v1.4.0

# Override target path for workspace build before include
TARGET_PATH=../../target

# Use cargo --target-dir to build directly in the expected location
export CARGO_TARGET_DIR := $(PROJ_DIR)target

# Use system oniguruma library instead of building from source to avoid GCC14 incompatibility
# export RUSTONIG_SYSTEM_LIBONIG := 1

# Enable tracing feature for CLI dependency
export CARGO_FEATURES :=
CARGO_OVERRIDE_DUCKDB_RS_FLAG :=

export V8_FROM_SOURCE := 1
export EXTRA_GN_ARGS := v8_monolithic=true v8_monolithic_for_shared_library=true 

all: configure debug

# Include makefiles from DuckDB
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_debug
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_rust
clean_all: clean_configure clean
