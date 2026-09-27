#!/usr/bin/env bash
#
# Local demo / e2e environment: Oxigraph seeded with the smoke fixture, plus a
# real Metabase loading the locally built driver jar, already set up with an
# admin and two SPARQL databases (auto sync and SHACL sync).
#
#   bin/demo.sh up     start (idempotent), then write target/demo/env.json
#   bin/demo.sh down   stop and remove the containers
#
# target/demo/env.json ({url, email, password, databases: {auto, shacl}}) is the
# contract for anything that drives this environment (metabase_api_test.clj,
# a future browser suite). Requires Docker (Compose v2), curl and python3.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

COMPOSE="$COMPOSE --profile demo"
JAR="target/sparql.metabase-driver.jar"
export MB_URL="http://localhost:3000"
export ENV_FILE="target/demo/env.json"
# Seen from the Metabase container, Oxigraph is `oxigraph`, not localhost.
export MB_ENDPOINT="${ENDPOINT/localhost/oxigraph}"
export MB_SHACL_URL="${SHACL_URL/localhost/oxigraph}"
export GRAPH

up() {
  if [ ! -f "$JAR" ]; then
    echo "FAILED: $JAR not found. Run 'make build' first." >&2
    exit 1
  fi

  echo "==> Starting Oxigraph and Metabase"
  $COMPOSE up -d
  wait_for_oxigraph
  seed_fixtures

  echo -n "==> Waiting for Metabase (first start takes a minute or two) "
  for _ in $(seq 1 180); do
    if curl -sf "$MB_URL/api/health" | grep -q '"ok"'; then echo " ok"; break; fi
    echo -n "."
    sleep 1
  done
  if ! curl -sf "$MB_URL/api/health" | grep -q '"ok"'; then
    echo " FAILED: Metabase never became healthy" >&2
    $COMPOSE logs metabase | tail -50 >&2 || true
    exit 1
  fi

  echo "==> Setting up Metabase"
  mkdir -p "$(dirname "$ENV_FILE")"
  python3 - <<'PY'
import json, os, time, urllib.request

url = os.environ["MB_URL"]
email, password = "admin@example.org", "Sparql-demo-2026"

def api(method, path, body=None, session=None):
    req = urllib.request.Request(url + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    if session:
        req.add_header("X-Metabase-Session", session)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.load(resp)

props = api("GET", "/api/session/properties")
if not props.get("has-user-setup"):
    session = api("POST", "/api/setup", {
        "token": props["setup-token"],
        "user": {"email": email, "password": password, "first_name": "Demo",
                 "last_name": "Admin", "site_name": "SPARQL driver demo"},
        "prefs": {"site_name": "SPARQL driver demo", "allow_tracking": False},
    })["id"]
else:
    session = api("POST", "/api/session", {"username": email, "password": password})["id"]

base = {"endpoint": os.environ["MB_ENDPOINT"], "default-graph": os.environ["GRAPH"]}
wanted = {
    "auto": ("SPARQL auto", base),
    "shacl": ("SPARQL SHACL", {**base, "advanced-options": True,
                               "metadata-sync-strategy": "shacl",
                               "shacl-url": os.environ["MB_SHACL_URL"]}),
}
existing = {d["name"]: d["id"] for d in api("GET", "/api/database", session=session)["data"]}
ids = {}
for key, (name, details) in wanted.items():
    ids[key] = existing.get(name) or api("POST", "/api/database",
                                         {"engine": "sparql", "name": name, "details": details},
                                         session=session)["id"]

for key, db_id in ids.items():
    for _ in range(120):
        if api("GET", f"/api/database/{db_id}", session=session)["initial_sync_status"] == "complete":
            break
        time.sleep(1)
    else:
        raise SystemExit(f"FAILED: database {key} ({db_id}) never finished its initial sync")

# The post-sync hook (dimensions.clj) runs after initial_sync_status is already
# complete: wait until it has written the rdfs:label display name and the FK
# remap.
LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
for _ in range(60):
    fields = [f for t in api("GET", f"/api/database/{ids['shacl']}/metadata", session=session)["tables"]
              for f in t["fields"]]
    remapped = any(api("GET", f"/api/field/{f['id']}", session=session)["dimensions"]
                   for f in fields if f.get("fk_target_field_id"))
    if remapped and all(f["display_name"] == "Label" for f in fields if f["name"] == LABEL):
        break
    time.sleep(1)
else:
    raise SystemExit("FAILED: the post-sync hook never wrote the FK remap and display names")

with open(os.environ["ENV_FILE"], "w") as f:
    json.dump({"url": url, "email": email, "password": password, "databases": ids}, f, indent=2)
print(f"==> Ready: {url}  (login {email} / {password})")
PY
}

down() {
  echo "==> Stopping Oxigraph and Metabase"
  $COMPOSE down -v
  rm -f "$ENV_FILE"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  *) echo "usage: $0 up|down" >&2; exit 2 ;;
esac
