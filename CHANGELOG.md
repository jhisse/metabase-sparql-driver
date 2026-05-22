# Changelog

All notable changes to the Metabase SPARQL Driver are documented in this file.

## [0.0.12] - 2026-05-21

### Fixed

- **Multi-hop FK joins** — chained joins (`Item → Provider → Owner`) now emit
  the intermediate `OPTIONAL { ?Provider_subject <…/owner> ?Owner_subject . }`
  triple, correctly anchored on the previous join's intermediate variable
  instead of `?subject`. Previously every FK triple was anchored on `?subject`,
  so a chained join left the second hop unbound and aggregates silently
  collapsed into the "no match" bucket. The source variable is now resolved
  in this order: the `:join-alias` on the FK field token in the join
  `:condition` (the direct signal for explicit chained joins from the notebook
  editor) → the FK field's `:table-id` mapped against prior joins'
  `:source-table` (for implicit chained joins) → `?subject` (single-hop default).
- **FK display columns rendered empty** — when Lib's `result-metadata`
  strips `:lib/join-alias` from implicit-joinable columns (keeping only
  `:fk-field-id`), the SELECT projection now recovers the originating join
  via `:fk-field-id` and projects the qualified `?<Alias>__<prop>` var
  instead of the unqualified `?<prop>`. Previously the display column was
  bound by the join target triple but the SELECT picked an unrelated
  base-table var of the same name, so the UI saw an empty column. Also
  prevents a bogus `OPTIONAL { ?subject <foreign-prop> ?var . }` from being
  emitted for fids that belong to a joined entity rather than the base table.
- **Native query parameters** — `{{tag}}` placeholders in native SPARQL are now
  rendered as syntactically valid SPARQL terms: text values are quoted and
  escaped, URL-shaped values are wrapped in `<…>`, numbers and booleans render
  as bare literals, and multi-value parameters become comma-separated term
  lists. Missing optional values leave the placeholder untouched and emit a
  warning instead of producing an invalid query. Whitespace inside `{{ tag }}`
  is tolerated.

### Added

- **SHACL `metabase:displayValueProperty` → Metabase Dimension** — after each
  metadata sync the driver writes (and idempotently updates) external-remap
  `Dimension` rows for every FK property that declares a display value in
  SHACL, so users no longer have to pick the display field manually in the
  column-settings panel.

### Changed

- Consolidated shared URI helpers (`shorten-uri`, `foreign-uri?`) into
  `metabase.driver.sparql.uri`. Extracted `emit-optional-triple` and
  `joined-var-name` helpers in `metabase.driver.sparql.mbql` so SPARQL triple
  emission and joined-var naming go through one path.

## [0.0.11] - 2026-05-16

First release since `v0.0.10`. This release adds SHACL-based metadata sync,
aggregations, foreign keys, language handling, and brings the driver up to
Metabase v0.61.1.

### Added

- **SHACL schema sync** — a new `shacl` metadata sync strategy. The driver
  fetches a SHACL document over HTTP and uses it as the single source of truth
  for tables, columns, datatypes, foreign keys, column order, required flags,
  and descriptions. Supports shape inheritance via `sh:node` and a custom
  `metabase:` vocabulary (`metabase:hide`, `metabase:semanticType`,
  `metabase:displayValueProperty`).
- **Aggregations** — Query Builder "Summarize" support for Count,
  Count distinct, Sum, Average, Minimum, and Maximum, with an optional
  group-by (breakout). `Count` compiles to `COUNT(DISTINCT ?subject)` so
  grouping by a multi-valued property counts entities, not fanned-out rows.
- **Foreign keys** — properties typed with `sh:class` become foreign keys; the
  driver emits `describe-fks` rows so Metabase wires up joins and value
  lookups automatically. Left-join semantics for FK columns.
- **Synthetic primary key** — every table exposes a `subject` primary-key
  column holding the RDF subject URI of each row.
- **Default Language** — a new connection setting. When set, queries against
  `rdf:langString` columns are filtered to that language (untagged literals
  kept), preventing multilingual row fan-out, and SHACL `sh:name` /
  `sh:description` labels resolve to it first.
- **Implicit base prefix** — the Default Graph URI doubles as a base prefix;
  classes and properties under it are shown by their local name in the UI and
  reconstructed to full URIs at query time.
- **Hide URIs outside the Default Graph** — a new option to skip
  foreign-namespace classes and properties during sync.
- **Saved cards / models as a query source** — the inner query is compiled as
  a SPARQL sub-`SELECT`, with foreign-key display-value remaps applied by the
  outer stage.

### Changed

- **Metabase compatibility** — updated for Metabase v0.61.1 (the v0.60 line is
  also covered). Adapts to the post-0.56 driver-API changes.
- The synthetic primary-key column was renamed from `id` to `subject`.
- Expanded the test suite (conversion, database, features, MBQL, and SHACL
  tests).
- Documentation overhaul covering all of the above.

### Upgrade notes

Upgrading from `v0.0.10` or earlier renames the synthetic primary-key column
from `id` to `subject` and shortens class/property names under the Default
Graph URI. After dropping in the new jar, **re-sync each database** so the
renamed metadata lands cleanly.

[0.0.12]: https://github.com/libis/metabase-sparql-driver/releases/tag/v0.0.12
