#!/usr/bin/env bash
#
# Smoke / integration harness: bring up an ephemeral Oxigraph SPARQL endpoint,
# seed it with the test fixture and its SHACL shapes, run the :integration-tagged tests against it,
# and always tear the container down.
#
# Invoked by `make smoke`. Requires Docker (with Compose v2), curl, python3 and a
# Java 21+ `clojure` on PATH. The metabase/ submodule must be initialized (make init-metabase).
set -euo pipefail

# Run from the repo root regardless of where the script is called from.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE="docker compose -f docker-compose.test.yml"
ENDPOINT="http://localhost:7878/query"
FIXTURE="test/resources/fixtures/smoke.ttl"
SHAPES="test/resources/fixtures/smoke-shapes.ttl"

# Single source of truth for the seed/query graph. The fixture is loaded into this
# NAMED graph so it matches the driver's :default-graph detail (sent as the
# ?default-graph-uri protocol param on every request). We export it to the test
# process (test_util.clj reads SPARQL_TEST_GRAPH) and derive the URL-encoded
# Graph Store Protocol target from the same value, so the two never drift.
GRAPH="${SPARQL_TEST_GRAPH:-https://example.org/}"
# The SHACL shapes live in a separate named graph, so they never show up in the
# data graph. Its Graph Store URL returns the shapes as Turtle on GET, which is
# exactly what the driver's `shacl-url` fetches; exported as SPARQL_TEST_SHACL_URL.
SHAPES_GRAPH="https://example.org/shapes"

store_url() {
  echo "http://localhost:7878/store?graph=$(python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1")"
}
STORE="$(store_url "$GRAPH")"
SHACL_URL="$(store_url "$SHAPES_GRAPH")"

teardown() {
  echo "==> Tearing down Oxigraph"
  $COMPOSE down -v >/dev/null 2>&1 || true
}
trap teardown EXIT

echo "==> Starting Oxigraph"
$COMPOSE up -d

echo -n "==> Waiting for the endpoint to answer ASK{} "
ready=""
for _ in $(seq 1 60); do
  code="$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    --data-urlencode 'query=ASK{}' "$ENDPOINT" || true)"
  if [ "$code" = "200" ]; then ready="yes"; echo " ok"; break; fi
  echo -n "."
  sleep 1
done
if [ -z "$ready" ]; then
  echo " FAILED: endpoint never became ready" >&2
  $COMPOSE logs >&2 || true
  exit 1
fi

seed() { # seed <file> <graph> <store-url>
  echo "==> Seeding $1 into graph <$2>"
  if ! curl -sS -f -X POST -H 'Content-Type: text/turtle' --data-binary "@$1" "$3"; then
    echo "FAILED: seeding $1 failed (see response above)" >&2
    $COMPOSE logs >&2 || true
    exit 1
  fi
}
seed "$FIXTURE" "$GRAPH" "$STORE"
seed "$SHAPES" "$SHAPES_GRAPH" "$SHACL_URL"

echo "==> Running :integration tests"
SPARQL_TEST_ENDPOINT="$ENDPOINT" SPARQL_TEST_GRAPH="$GRAPH" SPARQL_TEST_SHACL_URL="$SHACL_URL" \
  clojure -X:test :includes '[:integration]'
