PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Default target
.DEFAULT_GOAL := all

# Main extension configuration
EXTENSION_NAME=cql2elm

# Set to 0 to use stable C API (recommended for production)
USE_UNSTABLE_C_API=0

# The DuckDB version to target
TARGET_DUCKDB_VERSION=v1.2.0

# Configuration of extension (keep for cql2elm native build)
EXT_NAME=cql2elm
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

CQL2ELM_DIR:=cql2elm-be
CQL2ELM_TARGET:=$(CQL2ELM_DIR)/target
CQL2ELM_JAR:=$(CQL2ELM_TARGET)/cql2elm-native.jar
# Updated to allow root-level override
ROOT_GRAAL_CONF_DIR:=${PROJ_DIR}graalvm-config
CQL2ELM_REFLECT:=$(ROOT_GRAAL_CONF_DIR)/reflect-config.json
CQL2ELM_RESOURCES:=$(ROOT_GRAAL_CONF_DIR)/resource-config.json
CQL2ELM_NATIVE_SO:=$(CQL2ELM_DIR)/native-libs/$(shell uname -s | tr A-Z a-z)-$(shell uname -m)/libcql2elm-native.so
CQL2ELM_INIT_PKGS:=org.cqframework,com.fasterxml.jackson,org.hl7.elm,org.hl7.fhir

RESOURCE_JSON := { "resources": [ {"pattern": ".*\\.json$$"}, {"pattern": ".*\\.xml$$"}, {"pattern": ".*\\.cql$$"}, {"pattern": ".*\\.xsd$$"} ] }

.PHONY: cql2elm-native configure debug release test test_debug test_release clean clean_all
# Replaced legacy native-image build with CMake delegation
cql2elm-native:
	@echo "[cql2elm-native] Delegating to CMake target cql2elm_native (embedding handled in extension_config.cmake)"
	@mkdir -p build/release
	@cd build/release && ( [ -f CMakeCache.txt ] || cmake ../.. ) && $(MAKE) -j1 cql2elm_native

all: configure release

# Include makefiles from DuckDB C API extensions
include ../extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include ../extension-ci-tools/makefiles/c_api_extensions/c_cpp.Makefile

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_debug
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_cmake
clean_all: clean clean_configure
