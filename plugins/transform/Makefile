.PHONY: clean clean_all

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=transform

# Required by duckdb-rs which relies on unstable C API
USE_UNSTABLE_C_API=1

TARGET_DUCKDB_VERSION=v1.4.4

# Pin test DuckDB to match target version
DUCKDB_TEST_VERSION=$(TARGET_DUCKDB_VERSION)

all: configure debug

include ../extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include ../extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

# Create .duckdb_extension symlink so the Python test runner can load .trex files
test: test_debug
test_debug: build_extension_library_debug build_extension_with_metadata_debug
	@ln -sf $(EXTENSION_NAME).trex build/debug/$(EXTENSION_NAME).duckdb_extension
	@$(TEST_RUNNER) --test-dir test/sql --external-extension build/debug/$(EXTENSION_NAME).duckdb_extension
test_release: build_extension_library_release build_extension_with_metadata_release
	@ln -sf $(EXTENSION_NAME).trex build/release/$(EXTENSION_NAME).duckdb_extension
	@$(TEST_RUNNER) --test-dir test/sql --external-extension build/release/$(EXTENSION_NAME).duckdb_extension

clean: clean_build clean_rust
clean_all: clean_configure clean
