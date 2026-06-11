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

## Before opening a PR

Run the full checklist (see also the PR template):

```bash
make lint
make splint
make test    # requires Java 21+ and the metabase/ submodule (make init-metabase)
make format  # if you changed Clojure sources
```

## AI-assisted contributions

AI-assisted contributions are welcome, with one condition: **you must fully understand the code you submit and be able to discuss any line of it in review**. The same scope and size rules above apply regardless of how the code was written.
