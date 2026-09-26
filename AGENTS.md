# AGENTS.md

All contribution rules live in [CONTRIBUTING.md](CONTRIBUTING.md).
Read it and follow it strictly before making any change. In particular:

- Always create your branch from up-to-date `main`.
- One feature per PR, roughly under 400 changed lines.
- Never bump the version in `resources/metabase-plugin.yaml`.

## Working in this repo

- Checks: `make lint`, `make splint`, `make test`, `make format`; `make smoke` runs the integration tests against an ephemeral Oxigraph endpoint (requires Docker)
- Tests require Java 21+ and the `metabase/` git submodule initialized (`make init-metabase`). If the active JDK is older, the run fails with `No matching method newVirtualThreadPerTaskExecutor` — point `JAVA_HOME`/`PATH` at a JDK 21 before running `make test`.
- If `git status` shows `M metabase`, the submodule checkout has drifted from the committed pointer: run `git submodule update --init metabase` to realign it (this does not change the pointer)
- Integration tests must be tagged `^:integration` AND skip themselves when `SPARQL_TEST_ENDPOINT` is unset (use `skip-without-live-endpoint` from `test/metabase/driver/sparql/test_util.clj`). The tag alone is not enough: `make coverage` ignores test selectors and runs every test namespace, so an unguarded integration test breaks CI.
- Tests that produce SPARQL must also assert it parses: `(is (nil? (tu/sparql-syntax-error q)) q)` (RDF4J parser, `test_util.clj`). Fragment checks with `str/includes?` pass on malformed queries. Compile pMBQL with `tu/compile-query`, which runs the QP preprocessing first; calling `driver/mbql->native` directly compiles queries the driver never receives (for example, date strings not yet wrapped as `:absolute-datetime`, or `time-interval` not yet desugared). `make coverage` fails below the `--fail-threshold` in `deps.edn`: raise it when coverage grows, never lower it.
- Tests and examples use only reserved example domains (RFC 2606/6761): `example.org` by default, `*.example` when a second distinct namespace is needed, `.invalid` for endpoints that must fail. Real vocabulary namespaces (`w3.org`, `xmlns.com`, …) are fine; never use a real third-party or institutional domain as fake data.
- Never modify the `metabase/` submodule pointer. The Metabase version it pins is the one the README "Compatibility" table must name (check with `git -C metabase describe --tags`)
- Source layout: driver code in `src/metabase/driver/sparql/`, tests mirror it in `test/`; connection properties are declared in `resources/metabase-plugin.yaml`
- MBQL → SPARQL compilation lives in `src/metabase/driver/sparql/mbql.clj`; result type coercion in `conversion.clj`; sync/schema discovery in `database.clj`, `shacl.clj`, `templates.clj`; native `{{tag}}` parameters in `parameters.clj`; the SHACL FK display-value remap (post-sync hook) in `dimensions.clj`; URI shortening and SPARQL escaping in `uri.clj`
- Generating SPARQL: never string-concatenate a user-supplied value straight into a query. Route every literal through the one canonical string escaper (`uri/string-literal`) and every IRI through the shared `<iri>` helper (`uri/iri-ref`). Filters, custom expressions, and parameters all feed the same query string — an unescaped `"`, `\`, newline, or `>` breaks the query or allows injection.
- Before flipping a driver feature flag in `sparql.clj`, check the `metabase/` submodule for which MBQL clauses that feature gates (frontend clause `requiresFeature`), and confirm the compiler handles each one.
