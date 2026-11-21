# Shared by bin/smoke-test.sh and bin/demo.sh: the compose stack of the engine
# under test, the fixture locations, and the helpers that wait for the endpoint
# and seed it. Source it; it changes into the repo root.
#
# SPARQL_ENGINE (default oxigraph) picks the engine: its compose service and
# profile (both named after it), ENDPOINT, store_url (its Graph Store Protocol
# URL for a graph), and optionally a probe override.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SPARQL_ENGINE="${SPARQL_ENGINE:-oxigraph}"

urlencode() {
  python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"
}

# Succeeds once the endpoint answers ASK{}.
probe() {
  [ "$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    --data-urlencode 'query=ASK{}' "$ENDPOINT" || true)" = "200" ]
}

case "$SPARQL_ENGINE" in
  oxigraph)
    ENDPOINT="http://localhost:7878/query"
    store_url() { echo "http://localhost:7878/store?graph=$(urlencode "$1")"; }
    ;;
  *)
    echo "FAILED: unknown SPARQL_ENGINE '$SPARQL_ENGINE' (known: oxigraph)" >&2
    exit 2
    ;;
esac

COMPOSE="docker compose -f docker-compose.test.yml --profile $SPARQL_ENGINE"
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

STORE="$(store_url "$GRAPH")"
SHACL_URL="$(store_url "$SHAPES_GRAPH")"

wait_for_endpoint() {
  echo -n "==> Waiting for $SPARQL_ENGINE to answer "
  for _ in $(seq 1 60); do
    if probe; then echo " ok"; return; fi
    echo -n "."
    sleep 1
  done
  echo " FAILED: endpoint never became ready" >&2
  $COMPOSE logs >&2 || true
  exit 1
}

seed() { # seed <file> <graph> <store-url>
  echo "==> Seeding $1 into graph <$2>"
  if ! curl -sS -f -X POST -H 'Content-Type: text/turtle' --data-binary "@$1" "$3"; then
    echo "FAILED: seeding $1 failed (see response above)" >&2
    $COMPOSE logs >&2 || true
    exit 1
  fi
}

seed_fixtures() {
  # Graph Store POST appends: clear both graphs so a rerun reloads the fixture as is.
  curl -sS -f -X DELETE "$STORE" >/dev/null 2>&1 || true
  curl -sS -f -X DELETE "$SHACL_URL" >/dev/null 2>&1 || true
  seed "$FIXTURE" "$GRAPH" "$STORE"
  seed "$SHAPES" "$SHAPES_GRAPH" "$SHACL_URL"
}
