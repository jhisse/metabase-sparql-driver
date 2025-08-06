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

.PHONY: build clean init-metabase help all lint format splint test

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

# Lint code
lint:
	@echo "Linting code..."
	clojure -M:dev:lint
	@echo "Linting completed."

# Format code
format:
	@echo "Formatting code..."
	clojure -M:dev:format
	@echo "Formatting completed."

# Run splint for static code analysis
splint:
	@echo "Running splint static code analysis..."
	clojure -M:splint
	@echo "Splint analysis completed."

# Run tests
test:
	@echo "Running tests..."
	clojure -X:test
	@echo "Tests completed."

# Build docker image
docker-build:
	@echo "Building docker image..."
	@docker build -t metabase-sparql .
	@echo "Docker image built."

# Run docker image
docker-run:
	@echo "Running docker image..."
	@docker run -d -p 3000:3000 --name metabase-sparql metabase-sparql
	@echo "Docker image run."

# Stop docker image
docker-stop:
	@echo "Stopping docker image..."
	@docker stop metabase-sparql
	@echo "Docker image stopped."

# Clean old container
docker-clean:
	@echo "Cleaning old container..."
	@docker rm -f metabase-sparql
	@echo "Old container cleaned."

# Build driver with docker
docker-build-driver:
	@echo "Building driver with docker..."
	@docker build --tag metabase-sparql-driver --target builder-base .
	@docker run -v $(TARGET_DIR):/app/target metabase-sparql-driver
	@echo "Driver built with docker."

# Help
help:
	@echo "Available commands:"
	@echo "  make build               - Build the SPARQL driver"
	@echo "  make build-full          - Complete build with checks and initialization"
	@echo "  make clean               - Remove build files"
	@echo "  make init-metabase       - Initialize the Metabase submodule"
	@echo "  make check-deps          - Check if dependencies are installed"
	@echo "  make lint                - Lint code using clj-kondo"
	@echo "  make format              - Format code using cljfmt"
	@echo "  make splint              - Run splint static code analysis"
	@echo "  make test                - Run tests"
	@echo "  make docker-build        - Build docker image"
	@echo "  make docker-run          - Run docker image"
	@echo "  make docker-stop         - Stop docker image"
	@echo "  make docker-clean        - Clean old container"
	@echo "  make docker-build-driver - Build driver with docker"
	@echo "  make help                - Display this help"
	@echo ""
	@echo "Paths:"
	@echo "  Driver: $(DRIVER_PATH)"
	@echo "  Metabase: $(METABASE_PATH)"
	@echo "  Target: $(TARGET_DIR)"
