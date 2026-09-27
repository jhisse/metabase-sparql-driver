# Contributing

Thanks for your interest in improving the SPARQL driver. To keep the project reviewable and maintainable, contributions follow these rules.

## Pull requests

- **Always branch from up-to-date `main`**: `git fetch origin && git checkout -b feat/my-change origin/main`. PRs based on stale branches, or carrying commits already merged to `main`, will be closed.
- **One feature or fix per PR**, ideally under ~400 changed lines. Large multi-feature PRs will be closed with a request to split.
- For larger features (new sync strategies, type detection heuristics, geospatial support, query optimizations), **discuss the design with the maintainer first** before implementing.
- **PR descriptions must describe exactly what the diff contains** — nothing more, nothing less.

## Not accepted in contributor PRs

- Version bumps in `resources/metabase-plugin.yaml`. Versioning is a release decision made by the maintainer.
- Changes to the `metabase/` submodule pointer (unless intentional and explained in the PR).
- Generated reference docs, or docs that hardcode source line numbers. Code docstrings are the reference.
- Unrelated churn: rewriting README examples, reformatting untouched files, renaming things outside the scope of the change.

## Keeping docs accurate

If your change affects user-visible behavior (a connection property, a type mapping, a supported feature, SHACL handling), update the matching README section and the docstrings that describe it in the same PR. Outdated docs count as a bug.

## Code style

### Docstrings

- Every public var has a docstring. Private functions have one unless the name already says everything.
- Write prose. Don't use `Parameters:`, `Returns:` or `Usage:` sections.
- The first sentence says what the function returns or does. Start with a verb ("Render …", "Resolve …"), or with "True when …" for a predicate.
- Put argument names and code in backticks (`class-uri`) and refer to other vars as `[[name]]`. Describe the shape of the return value inline (`{:vars [...] :triples [...]}`).
- After that, write only what the code does not show: why it works this way, edge cases, what happens on failure (returns nil or throws), and SPARQL or Metabase constraints.
- Don't list callers and don't repeat the body. Both go stale.
- Known limitations and upgrade paths go in plain `;;` comments next to the code, not in the docstring.

### Names

- Top-level `def`s are `^:private`. Anything shared across namespaces is a function.
- IRIs and datatype URIs are named constants (`xsd-date`, `sh`), never inline strings.
- Conversions are named `a->b`, predicates end in `?`, and functions with side effects end in `!`.

## Before opening a PR

Run the full checklist (see also the PR template):

```bash
make lint
make splint
make test    # requires Java 21+ and the metabase/ submodule (make init-metabase)
make smoke   # integration tests against an ephemeral Oxigraph endpoint (requires Docker, curl, python3)
make e2e     # optional: tests through a real Metabase (make build first; requires Docker, curl, python3)
make format  # if you changed Clojure sources
```

## AI-assisted contributions

AI-assisted contributions are welcome, with one condition: **you must fully understand the code you submit and be able to discuss any line of it in review**. The same scope and size rules above apply regardless of how the code was written.
