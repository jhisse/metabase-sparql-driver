# Metabase SPARQL Driver

A driver for connecting Metabase to SPARQL endpoints for querying RDF data.

## Overview

This driver enables Metabase to connect to SPARQL endpoints using HTTP requests to query RDF data. It supports both secure and insecure connections with optional default graph specification.

This driver follow the (SPARQL 1.1 Query Results JSON Format)[https://www.w3.org/TR/2013/REC-sparql11-results-json-20130321/] specification.

## Build Instructions

### Prerequisites

- Git
- Clojure CLI tools
- Java Development Kit (JDK) 21

### Building the Driver JAR

1. Clone this repository:

   ```bash
   git clone https://github.com/yourusername/metabase-sparql-driver.git
   cd metabase-sparql-driver
   ```

2. Initialize the Metabase submodule:

   ```bash
   make init-metabase
   ```

3. Build the driver:

   ```bash
   make build
   ```

4. The compiled driver will be available at:

   ```text
   target/sparql.metabase-driver.jar
   ```

### Additional Make Commands

- `make clean` - Remove build files
- `make check-deps` - Check if dependencies are installed
- `make build-full` - Complete build with checks and initialization
- `make help` - Display help information

## Installation

1. Stop your Metabase instance if it's running
2. Copy the `sparql.metabase-driver.jar` file to the Metabase plugins directory:

   ```bash
   cp target/sparql.metabase-driver.jar /path/to/metabase/plugins/
   ```

3. Restart Metabase
4. The SPARQL driver should now be available when adding a new database

## Configuration

When connecting to a SPARQL endpoint, you'll need to provide:

- **Endpoint URL** (required): The URL of the SPARQL endpoint (e.g., `http://dbpedia.org/sparql`).
- **Default Graph** (optional): URI of the default graph to query. Default is empty.
- **Ignore SSL certificate validation** (optional): Set to true to ignore SSL certificate validation for testing purposes. Default is false.

## License

This driver is licensed under the AGPLv3 license. See the [LICENSE](LICENSE) file for details.
