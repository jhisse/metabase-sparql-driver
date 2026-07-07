#!/usr/bin/env bash
#
# Smoke / integration harness: bring up an ephemeral Oxigraph SPARQL endpoint,
# seed it with the test fixture, run the :integration-tagged tests against it,
# and always tear the container down.
#
# Invoked by `make smoke`. Requires Docker (with Compose v2) and a Java 21+
# `clojure` on PATH. The metabase/ submodule must be initialized (make init-metabase).
set -euo pipefail

# Run from the repo root regardless of where the script is called from.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE="docker compose -f docker-compose.test.yml"
ENDPOINT="http://localhost:7878/query"
FIXTURE="test/resources/fixtures/smoke.ttl"

# Single source of truth for the seed/query graph. The fixture is loaded into this
# NAMED graph so it matches the driver's :default-graph detail (sent as the
# ?default-graph-uri protocol param on every request). We export it to the test
# process (smoke_test.clj reads SPARQL_TEST_GRAPH) and derive the URL-encoded
# Graph Store Protocol target from the same value, so the two never drift.
GRAPH="${SPARQL_TEST_GRAPH:-https://example.org/}"
GRAPH_ENC="$(python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=""))' "$GRAPH")"
STORE="http://localhost:7878/store?graph=$GRAPH_ENC"

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

echo "==> Seeding fixture ($FIXTURE) into graph <$GRAPH>"
if ! curl -sS -f -X POST -H 'Content-Type: text/turtle' \
     --data-binary "@$FIXTURE" "$STORE"; then
  echo "FAILED: seeding the fixture failed (see response above)" >&2
  $COMPOSE logs >&2 || true
  exit 1
fi

echo "==> Running :integration tests"
SPARQL_TEST_ENDPOINT="$ENDPOINT" SPARQL_TEST_GRAPH="$GRAPH" \
  clojure -X:test :includes '[:integration]'
