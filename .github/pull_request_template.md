## Summary

<!-- What does this PR change and why? -->

## Checklist

- [ ] `make lint` passes
- [ ] `make splint` passes
- [ ] `make test` passes
- [ ] `make smoke` passes (requires Docker)
- [ ] `make e2e` passes, if you changed sync, the query path or the demo setup (requires Docker and `make build`)
- [ ] `make format` applied (if you changed Clojure sources)
- [ ] `metabase/` submodule pointer is unchanged (or the bump is intentional and explained above)
- [ ] No build artifacts staged (`target/`, `.clj-kondo/.cache/`, `.cpcache/`)
