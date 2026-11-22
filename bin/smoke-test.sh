#!/usr/bin/env bash
#
# Smoke / integration harness: bring up an ephemeral SPARQL endpoint (the
# SPARQL_ENGINE from bin/lib.sh, Oxigraph by default), seed it with the test
# fixture and its SHACL shapes, run the :integration-tagged tests against it,
# and always tear the container down.
#
# Invoked by `make smoke`. Requires Docker (with Compose v2), curl, python3 and a
# Java 25+ `clojure` on PATH. The metabase/ submodule must be initialized (make init-metabase).
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

teardown() {
  echo "==> Tearing down $SPARQL_ENGINE"
  $COMPOSE down -v >/dev/null 2>&1 || true
}
# `make demo` shares this compose project and runs on Oxigraph: reuse its
# endpoint and leave it running. Any other running engine is a leftover, torn down.
if [ "$SPARQL_ENGINE" = oxigraph ] && [ -n "$($COMPOSE ps -q oxigraph 2>/dev/null)" ]; then
  echo "==> $SPARQL_ENGINE already running (make demo?): reusing it, no teardown"
else
  trap teardown EXIT
fi

echo "==> Starting $SPARQL_ENGINE"
$COMPOSE up -d

wait_for_endpoint
seed_fixtures

echo "==> Running :integration tests"
SPARQL_TEST_ENDPOINT="$ENDPOINT" SPARQL_TEST_GRAPH="$GRAPH" SPARQL_TEST_SHACL_URL="$SHACL_URL" \
  clojure -X:test :includes '[:integration]'
