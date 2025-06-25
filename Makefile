# Makefile for building the SPARQL driver for Metabase

# Makefile directory (where this file is located)
MAKEFILE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
MAKEFILE_DIR := $(patsubst %/,%,$(MAKEFILE_DIR))

# Driver directory (current)
DRIVER_PATH := $(MAKEFILE_DIR)

# Metabase directory (relative to Makefile)
METABASE_PATH := $(MAKEFILE_DIR)/metabase

# Target directory for the compiled driver
TARGET_DIR := $(DRIVER_PATH)/target

.PHONY: build clean init-metabase help all

# Default rule
all: build

# Build the driver
build:
	@echo "Building SPARQL driver for Metabase..."
	@echo "Driver path: $(DRIVER_PATH)"
	@echo "Metabase path: $(METABASE_PATH)"
	@if [ ! -d "$(METABASE_PATH)" ]; then \
		echo "ERROR: Metabase directory not found: $(METABASE_PATH)"; \
		echo "Run 'make init-metabase' first."; \
		exit 1; \
	fi
	@mkdir -p $(TARGET_DIR)
	@cd $(METABASE_PATH) && \
		clojure \
		-Sdeps "{:aliases {:sparql {:extra-deps {com.metabase/sparql-driver {:local/root \"$(DRIVER_PATH)\"}}}}}" \
		-X:build:sparql \
		build-drivers.build-driver/build-driver! \
		"{:driver :sparql, :project-dir \"$(DRIVER_PATH)\", :target-dir \"$(TARGET_DIR)\"}"
	@echo "Driver successfully built at $(TARGET_DIR)/sparql.metabase-driver.jar"

# Clean build files
clean:
	@echo "Cleaning build files..."
	@rm -rf $(TARGET_DIR)
	@echo "Build files removed."

# Initialize and update Metabase submodule if necessary
init-metabase:
	@echo "Initializing Metabase submodule..."
	@if [ ! -f .gitmodules ]; then \
		echo "WARNING: .gitmodules file not found. Make sure the submodule is configured."; \
	fi
	@git submodule update --init --recursive
	@echo "Metabase submodule initialized."

# Check dependencies
check-deps:
	@echo "Checking dependencies..."
	@command -v clojure >/dev/null 2>&1 || { echo "ERROR: Clojure is not installed."; exit 1; }
	@command -v git >/dev/null 2>&1 || { echo "ERROR: Git is not installed."; exit 1; }
	@echo "All dependencies are available."

# Complete build (with checks)
build-full: check-deps init-metabase build

# Help
help:
	@echo "Available commands:"
	@echo "  make build        - Build the SPARQL driver"
	@echo "  make build-full   - Complete build with checks and initialization"
	@echo "  make clean        - Remove build files"
	@echo "  make init-metabase- Initialize the Metabase submodule"
	@echo "  make check-deps   - Check if dependencies are installed"
	@echo "  make help         - Display this help"
	@echo ""
	@echo "Paths:"
	@echo "  Driver: $(DRIVER_PATH)"
	@echo "  Metabase: $(METABASE_PATH)"
	@echo "  Target: $(TARGET_DIR)"