# AGENTS.md

All contribution rules live in [CONTRIBUTING.md](CONTRIBUTING.md).
Read it and follow it strictly before making any change. In particular:

- Always create your branch from up-to-date `main`.
- One feature per PR, roughly under 400 changed lines.
- Never bump the version in `resources/metabase-plugin.yaml`.

## Working in this repo

- Checks: `make lint`, `make splint`, `make test`, `make format`; `make smoke` runs the integration tests against an ephemeral Oxigraph endpoint (requires Docker)
- Tests require Java 21+ and the `metabase/` git submodule initialized (`make init-metabase`)
- Integration tests must be tagged `^:integration` AND skip themselves when `SPARQL_TEST_ENDPOINT` is unset (use `skip-without-live-endpoint` from `test/metabase/driver/sparql/test_util.clj`). The tag alone is not enough: `make coverage` ignores test selectors and runs every test namespace, so an unguarded integration test breaks CI.
- Never modify the `metabase/` submodule pointer
- Source layout: driver code in `src/metabase/driver/sparql/`, tests mirror it in `test/`
- MBQL → SPARQL compilation lives in `src/metabase/driver/sparql/mbql.clj`; result type coercion in `conversion.clj`; sync/schema discovery in `database.clj`, `shacl.clj`, `templates.clj`
