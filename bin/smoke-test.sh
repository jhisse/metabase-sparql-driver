#!/usr/bin/env bash
#
# Smoke / integration harness: bring up an ephemeral Oxigraph SPARQL endpoint,
# seed it with the test fixture and its SHACL shapes, run the :integration-tagged tests against it,
# and always tear the container down.
#
# Invoked by `make smoke`. Requires Docker (with Compose v2), curl, python3 and a
# Java 21+ `clojure` on PATH. The metabase/ submodule must be initialized (make init-metabase).
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

teardown() {
  echo "==> Tearing down Oxigraph"
  $COMPOSE down -v >/dev/null 2>&1 || true
}
trap teardown EXIT

echo "==> Starting Oxigraph"
$COMPOSE up -d

wait_for_oxigraph
seed_fixtures

echo "==> Running :integration tests"
SPARQL_TEST_ENDPOINT="$ENDPOINT" SPARQL_TEST_GRAPH="$GRAPH" SPARQL_TEST_SHACL_URL="$SHACL_URL" \
  clojure -X:test :includes '[:integration]'
