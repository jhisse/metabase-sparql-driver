# SPARQL Query Builder — Developer Reference

How the `metabase-sparql-driver` compiles a Metabase query into SPARQL, and the
conventions that compiled output follows. Read this before changing the
transpiler, or before writing a native SPARQL query that has to coexist with
generated ones.

The authoritative implementation is [`src/metabase/driver/sparql/mbql.clj`](src/metabase/driver/sparql/mbql.clj);
this document explains *what* it emits and *why*. When the two disagree, the
code wins — update this file.

---

## 1. Where things live

| Concern | Namespace |
|:--|:--|
| MBQL → SPARQL transpilation (the bulk of this doc) | [`sparql/mbql.clj`](src/metabase/driver/sparql/mbql.clj) |
| `{{tag}}` parameter substitution in native queries | [`sparql/parameters.clj`](src/metabase/driver/sparql/parameters.clj) |
| URI shortening / reconstruction against the Default Graph | [`sparql/uri.clj`](src/metabase/driver/sparql/uri.clj) |
| Sync queries (class/property discovery, capability probes) | [`sparql/templates.clj`](src/metabase/driver/sparql/templates.clj) |
| SHACL-driven schema sync (langString, FK display values, types) | [`sparql/shacl.clj`](src/metabase/driver/sparql/shacl.clj) |
| Result term → Metabase value coercion, geometry/WKT detection | [`sparql/conversion.clj`](src/metabase/driver/sparql/conversion.clj) |
| Synthesized coordinate columns at sync time | [`sparql/database.clj`](src/metabase/driver/sparql/database.clj) |

The entry point is `mbql->native` at the bottom of `mbql.clj`. Everything else
in that file is a private helper feeding it.

---

## 2. The compilation pipeline

`mbql->native` does three things ([mbql.clj:1407](src/metabase/driver/sparql/mbql.clj#L1407)):

1. **Ask Lib for the authoritative result columns** via
   `result-metadata/returned-columns` (`expected-result-columns`). This is the
   *same* calculation the `annotate` middleware uses, so the driver's SELECT can
   never drift from the column count/order Metabase expects. If Lib throws, the
   driver falls back to deriving the projection from the query's own `:fields`.
2. **Convert pMBQL (MBQL 5) → legacy MBQL** with `->legacy-MBQL`. All the
   internal helpers operate on legacy `[:field id opts]` tokens.
3. **Compile the stage tree** with `compile-stage`, passing `expected-cols` only
   to the *outermost* stage.

A stage is either:

- **base stage** (`:source-table` present) → `compile-base-stage`. Emits the
  triple patterns. This is where most of the logic lives.
- **derived stage** (`:source-query` present) → `compile-derived-stage`. Wraps
  the inner stage as a SPARQL sub-`SELECT` and layers remap joins / outer
  aggregation around it. Added by the QP for saved cards/models used as a
  source, or when an FK display-value remap sits on top of an aggregation.

`compile-stage` recurses through `:source-query` wrappers; inner sub-`SELECT`s
are compiled with `expected-cols = nil`.

### WHERE body assembly order

`compile-base-stage` concatenates the WHERE body in a fixed order
([mbql.clj:1183](src/metabase/driver/sparql/mbql.clj#L1183)). Preserve this order
when adding new emitters — downstream clauses depend on variables bound earlier:

```
?subject a <class> .          ; class anchor (always first)
anchor-triples                ; pushed-up equality constants (Principle: anchor selectivity)
anchor-binds                  ; BIND(<const> AS ?var) so the column stays projectable
triples-for-fields            ; OPTIONAL { ?subject <prop> ?var } for projected direct columns
triples-for-extras            ; same, for columns referenced only in filter/order-by
join-fk-triples               ; OPTIONAL { ?src <fk> ?alias_subject }
join-target-triples           ; OPTIONAL { ?alias_subject <prop> ?alias__field }
reconciled :triples           ; columns Lib expected that the compiler otherwise missed
geo-coord-triples / -binds    ; synthesized lon/lat source triple + extraction BIND
bin-binds                     ; FLOOR bucket BINDs
expr-bind-lines               ; custom-column BIND(... AS ?name)
lang-filter-lines             ; LANG() filters for langString columns
filters                       ; residual user FILTER (what wasn't pushed into an anchor)
```

Then `SELECT … WHERE { … } [GROUP BY] [ORDER BY] [LIMIT]`.

---

## 3. Naming conventions

Every variable name is run through `sanitize-var-name` (non-`[A-Za-z0-9_]` → `_`,
leading digit prefixed with `_`). The stable conventions are:

| Variable | Meaning | Source |
|:--|:--|:--|
| `?subject` | The synthetic primary key — the RDF subject URI of the row entity. Always projected; the join anchor. | `id-field?`, `build-pk-field` |
| `?<field>` | A direct column, named from the sanitized RDF property local name. | `build-var-aliases` |
| `?<alias>_subject` | The intermediate join variable — the joined entity's subject URI. | `alias->intermediate-var` |
| `?<alias>__<field>` | A column reached through a join (note the **double** underscore). Keeps namespaces distinct when several FK chains share property names. | `joined-var-name` |
| `?<var>_binned` | The bucket variable for a binned column. | `var-for-token` |
| `?ag_N` | The Nth aggregation expression. | `aggregation->projection` |
| `?undefined_N` | A column Lib expected that could not be resolved — projected unbound so the column count still matches; comes back as `nil`. | reconcile helpers |

**The subject column is identified by the reserved name `subject`, not by
semantic type.** `id-field?` deliberately does *not* key off `:type/PK`:
Metabase's name-based classifier stamps `:type/PK` on any field literally named
`id`, and after Default-Graph shortening a real RDF property `<base>/id` becomes
a column named `id`. Treating that as the subject would silently drop it.

---

## 4. IRIs and the Default Graph

Compiled queries use **inline, fully-qualified IRIs** (`<https://…>`) — never a
`PREFIX` block. A `PREFIX` declaration inside a sub-`SELECT` (which every inner
stage becomes) is invalid, so the driver never emits one.

`uri/absolute-uri` reconstructs a property/class IRI from a column name:

- A name that already carries a URI scheme (`http:`, `urn:`, …) is used verbatim.
- Otherwise it is treated as relative to the **Default Graph** prefix
  (`details.default_graph`) and that prefix is prepended.
- Blank name or blank Default Graph → returned unchanged.

`uri/shorten-uri` is the inverse, applied during sync so column names display
without the redundant base prefix. The round-trip is what lets a column named
`id` mean `<default-graph>id` at compile time.

---

## 5. The class anchor and anchor pushing

Every base stage starts its WHERE with the class triple:

```sparql
?subject a <http://dbpedia.org/ontology/Person> .
```

A bare class triple is **not selective** on large endpoints (it can match
millions of subjects). To give the engine an index lookup, the compiler
*pushes selective equality filters up* into mandatory BGP triples rather than
leaving them as a trailing `FILTER`.

`extract-anchors` ([mbql.clj:554](src/metabase/driver/sparql/mbql.clj#L554)) pulls
out every clause that `anchorable-clause?` accepts:

- a top-level `[:= <field> <const>]`, or such clauses among the children of a
  top-level `:and` (never from inside an `:or` — pushing would change semantics);
- on a **direct** field (no `:join-alias`), with a resolvable property;
- **not** the subject, **not** a `langString` column (language-equality semantics
  differ), **not** a geometry column (needs `STR()` comparison);
- with a non-nil concrete constant.

Each anchor becomes a mandatory triple plus a `BIND` so the column variable is
still available to the SELECT/ORDER BY:

```sparql
?subject <https://example.org/id> <https://example.org/ids/REC-99201> .
BIND(<https://example.org/ids/REC-99201> AS ?id)
```

Whatever is left over (`:residual`) becomes the bottom `FILTER`.

---

## 6. OPTIONALs: direct columns and FK chains

Direct columns are emitted as independent sibling OPTIONALs off `?subject`:

```sparql
OPTIONAL { ?subject <https://example.org/name> ?name . }
```

They are OPTIONAL (left-join semantics) so a missing property never drops the
entity. `emit-optional-triple` is the single choke point for this line shape.

**FK chains are flat-paired sibling OPTIONALs, not nested.** When a column is
reached through an implicit join (FK-remap display value), each hop anchors on
the *previous* hop's intermediate variable:

```sparql
OPTIONAL { ?subject       <…/birthPlace> ?Place_subject . }   ; FK triple
OPTIONAL { ?Place_subject <…/label>      ?Place__label . }    ; target triple
```

For a multi-hop chain (`Item → Provider → Owner`), `alias->source-var` resolves
each FK triple's left-hand side to the prior join's intermediate var, so the
chain stays connected instead of re-anchoring on `?subject` (which would create
a Cartesian product when an intermediate is multi-valued):

```sparql
OPTIONAL { ?subject          <…/provider> ?Provider_subject . }
OPTIONAL { ?Provider_subject <…/owner>    ?Owner_subject . }
OPTIONAL { ?Owner_subject    <…/name>     ?Owner__name . }
```

Source-var resolution order for a chained join
([mbql.clj:936](src/metabase/driver/sparql/mbql.clj#L936)): the FK token's own
`:join-alias` (explicit chained joins) → the FK field's parent `:table-id`
matching a previously-joined `:source-table` (implicit chained joins) → fall
back to `?subject` (with a warning if a parent table was expected but no prior
join produces it).

The joined entity's own subject column needs **no** triple — it *is* the
intermediate var, already bound by the FK triple (`pair->target-var` maps it
straight to `?<alias>_subject`).

---

## 7. Filters

`compile-filter-expr` ([mbql.clj:259](src/metabase/driver/sparql/mbql.clj#L259))
turns an MBQL filter clause into a boolean expression string. Boolean structure:
`:and` → `&&`, `:or` → `||`, `:not` → `!`.

| MBQL op | SPARQL emitted |
|:--|:--|
| `:=` (value) | `(?v = <term>)` — term is an IRI `<…>` for FK/URL fields, else a literal |
| `:=` (nil) | `(!BOUND(?v))` |
| `:=` (geometry field) | `(STR(?v) = "lit")` — see §10 |
| `:!=` | `(?v != …)` / `(BOUND(?v))` for nil / `STR()` for geometry |
| `:>` `:>=` `:<` `:<=` | numeric comparison |
| `:between` | `(?v >= min && ?v <= max)` |
| `:starts-with` / `:ends-with` / `:contains` | `STRSTARTS` / `STRENDS` / `CONTAINS` over `STR(?v)`; wrapped in `LCASE(…)` when `:case-sensitive false` |
| `:is-null` / `:not-null` | `(!BOUND(?v))` / `(BOUND(?v))` |
| `:inside` | bounding-box range over the lat/lon columns — see §10 |

**IRI-valued equality.** `value->term` emits `<value>` instead of a quoted
literal when the field is IRI-valued (`:type/FK`, `:type/URL`, or base type
`:type/URL`) *and* the value is URL-shaped. SPARQL binds those columns to IRI
nodes, so `= "https://…"` as a string literal would never match.

---

## 8. Language strings (`rdf:langString`)

A column whose SHACL `:database-type` is `"langString"` gets a per-variable
guard filter when a Default Language is configured (`lang-filter-line`,
[mbql.clj:114](src/metabase/driver/sparql/mbql.clj#L114)):

```sparql
FILTER(!BOUND(?title) || LANG(?title) = "nl" || LANG(?title) = "")
```

Three parts, all load-bearing:

- `!BOUND(?title)` — keeps the entity when the property is absent or has no
  matching translation (the column came through an OPTIONAL, so it may be
  unbound). Without it, a missing translation silently drops the whole row.
- `LANG(?title) = "nl"` — the configured Default Language.
- `LANG(?title) = ""` — accepts untagged literals.

These filters are emitted **after** the OPTIONAL chain and **before** any
residual user FILTER, one per langString variable (direct and joined). This is
the canonical shape; hand-written native queries should match it. Putting the
language check *inside* the OPTIONAL is equivalent for row semantics but drops
the `!BOUND` guard (the variable is bound by definition once the inner pattern
matches).

To *exclude* entities lacking a translation (rare — e.g. an export that only
ships one language), drop the `!BOUND` guard. The driver never does this
automatically.

---

## 9. Custom expressions (Metabase "custom columns")

`compile-expression` ([mbql.clj:427](src/metabase/driver/sparql/mbql.clj#L427))
maps the Metabase expression function subset to SPARQL, emitted as
`BIND(<expr> AS ?name)` after the triples that bind the referenced variables.

Supported: arithmetic (`+ - * /`), `abs ceil floor round`, string functions
(`length lower upper trim ltrim rtrim concat substring replace`,
`regex-match-first`), `coalesce`, casts (`float`/`double`, `integer`, `text`),
comparison/logical operators inside `:case` predicates, and `:case` itself
(→ nested `IF()`). An unsupported function throws `ex-info` so the query fails
loudly rather than silently dropping the column.

---

## 10. Geometry / WKT columns

A column synced as `:database-type "geometry"` holds a typed geometry literal
(e.g. `"POINT(...)"^^virtrdf:Geometry`). Two consequences:

**Equality must compare the lexical form.** A direct `?geom = "POINT(...)"`
never matches the typed term, so `:=`/`:!=` on a geometry field compile to
`STR(?geom) = "…"` (`geometry-field?` gate in `compile-filter-expr`). Geometry
fields are also excluded from anchor pushing for the same reason.

**Coordinate columns are synthesized at sync time.** `database.clj` stamps a
marker `:database-type "geo-coord:<axis>:<source>"` on driver-invented lon/lat
(and box-corner) columns. At compile time `geo-coord-emit`
([mbql.clj:825](src/metabase/driver/sparql/mbql.clj#L825)) emits, instead of a
property triple, the shared source-geometry triple plus a capture-group
`REPLACE` that extracts the coordinate and casts it to `xsd:double`:

```sparql
OPTIONAL { ?subject <…/geometry> ?geometry . }
BIND(<…#double>(REPLACE(STR(?geometry), "^.*POINT *\\(([-0-9.eE+]+) ([-0-9.eE+]+).*", "$1")) AS ?lon)
```

Axes: `point-lon`, `point-lat` (from `POINT(lon lat)`) and `box-min-lon`,
`box-max-lon`, `box-min-lat`, `box-max-lat` (from Virtuoso `BOX(...)`).

**The map "draw a box" filter** (`:inside`) compiles to a bounding-box range
over the two coordinate columns:

```sparql
FILTER(((?lat <= N) && (?lat >= S) && (?lon >= W) && (?lon <= E)))
```

---

## 11. Binning (grid / heat maps, numeric binning)

A binned field carries resolved `:binning` opts (`:bin-width`, `:min-value`)
after the QP's binning middleware. `bin-expr`
([mbql.clj:849](src/metabase/driver/sparql/mbql.clj#L849)) buckets the underlying
column var with Metabase's canonical formula `floor((v - min) / width) * width + min`,
bound to a `?<var>_binned` variable that the query groups/selects on:

```sparql
BIND((FLOOR(?lat / 10) * 10) AS ?lat_binned)            ; min = 0
BIND(((FLOOR((?lon - -180) / 10) * 10) + -180) AS ?lon_binned)  ; min ≠ 0
```

---

## 12. Aggregations

When `:aggregation` is present the stage projects only breakout columns and
aggregate expressions, with a `GROUP BY` over the breakouts.

`[:count]` with **no argument** compiles to `COUNT(DISTINCT ?subject)` in a base
stage ([mbql.clj:671](src/metabase/driver/sparql/mbql.clj#L671)). This is
deliberate: multi-valued OPTIONALs fan an entity out into several solution rows,
and `COUNT(*)` would count the fanned-out rows. `COUNT(DISTINCT ?subject)` counts
entities. (In a *derived* stage there is no `?subject`, so an arg-less count
becomes `COUNT(*)` — `count-all?` flag.)

Other aggregations map directly: `:distinct` → `COUNT(DISTINCT ?arg)`, `:sum` →
`SUM`, `:avg` → `AVG`, `:min`/`:max` → `MIN`/`MAX`.

**Aggregation output names.** A later stage may reference an aggregation as a
plain field (e.g. drilling on a count value adds `[:< [:field "count" …] 12]`).
`aggregation-output-name` / `aggregation-name->var` map Metabase's default
result-column name (`count`, `sum`, `avg`, … with `_2`, `_3` dedupe) back to the
inner `?ag_N` variable so those outer references resolve.

---

## 13. Expected-columns reconciliation

For non-aggregation outermost stages the SELECT is reconciled against Lib's
`expected-cols` so the driver's column count and order can never drift from the
`annotate` middleware (`reconcile-base-projection`,
[mbql.clj:729](src/metabase/driver/sparql/mbql.clj#L729); the derived-stage
equivalent is inline in `compile-derived-stage`).

For each expected column, in Lib's order: reuse the variable the compiler
already bound; or synthesize a missing one (an extra OPTIONAL off the join
intermediate var or off `?subject`); or, if truly unresolvable (e.g. a
stray expression), project `?undefined_N` so the count still matches and the
value returns `nil`.

Note that Lib's result-metadata strips `:lib/join-alias` from implicitly-joinable
columns and stamps `:fk-field-id` instead; `fk-fid->alias` recovers the
originating join so the qualified variable can be looked up.

---

## 14. Native queries and `{{tag}}` parameters

`substitute-native-parameters` ([parameters.clj:99](src/metabase/driver/sparql/parameters.clj#L99))
replaces each `{{tag}}` with a SPARQL term rendered by type via `->sparql-term`:

| Parameter value | Rendered as |
|:--|:--|
| Text — `Alice` | `"Alice"` (quoted, escaped for `\ " \n \r \t`) |
| IRI-shaped — `https://…` or `urn:…` | `<https://…>` (angle brackets added) |
| Number — `25` | `25` |
| Boolean — `true` | `true` |
| Sequential `[A B C]` | `A, B, C` — only valid inside `IN(...)` / `VALUES`; the template author must wrap it |
| Missing / `no-value` | placeholder **left untouched**, warning logged (the endpoint then surfaces a clear parse error) |

Field filters, referenced cards/snippets/tables are unsupported and skipped with
a warning.

**Do not wrap the placeholder yourself.** An IRI-shaped value already renders
with angle brackets, so `<{{thing}}>` produces `<<…>>` — a parse error.
Likewise `"{{name}}"` double-quotes a string value. Write the template with the
bare placeholder and let the driver choose the term form:

```sparql
SELECT ?label WHERE {
  ?subject a {{ class }} ;                       # class is IRI-shaped → <…>
           <http://www.w3.org/2000/01/rdf-schema#label> ?label .
  FILTER(?nationality IN ({{ nationalities }}))  # multi-value → comma list inside IN()
}
LIMIT {{ n }}
```

Native queries written for humans *may* use a `PREFIX` block (they are not
wrapped in an outer SELECT) — but inline IRIs keep them consistent with
generated output.

---

## 15. Worked examples

### 15.1 Selective anchor + consolidated property path

A filter on a concrete identifier is pushed into a mandatory triple; the meta
path is consolidated to one OPTIONAL plus a flat dependent sibling.

```sparql
SELECT ?subject ?name ?val
WHERE {
  ?subject a <https://example.org/Record> .
  ?subject <https://example.org/id> <https://example.org/ids/REC-99201> .
  BIND(<https://example.org/ids/REC-99201> AS ?id)

  OPTIONAL { ?subject  <https://example.org/name>  ?name . }
  OPTIONAL { ?subject  <https://example.org/meta>  ?meta_obj . }
  OPTIONAL { ?meta_obj <https://example.org/value> ?val . }
}
```

### 15.2 langString column with the canonical guard

`rdf:langString` column, Default Language `nl`:

```sparql
SELECT ?subject ?label
WHERE {
  ?subject a <http://schema.org/Book> .
  OPTIONAL { ?subject <http://schema.org/name> ?label . }
  FILTER(!BOUND(?label) || LANG(?label) = "nl" || LANG(?label) = "")
}
```

### 15.3 FK display value (flat-paired OPTIONALs)

`Person.birthPlace` FK to `Place`, displaying `Place.label` (a langString):

```sparql
SELECT ?subject ?birthName ?Place_subject ?Place__label
WHERE {
  ?subject a <http://dbpedia.org/ontology/Person> .

  OPTIONAL { ?subject       <http://dbpedia.org/ontology/birthName>     ?birthName . }
  OPTIONAL { ?subject       <http://dbpedia.org/ontology/birthPlace>    ?Place_subject . }
  OPTIONAL { ?Place_subject <http://www.w3.org/2000/01/rdf-schema#label> ?Place__label . }

  FILTER(!BOUND(?birthName)    || LANG(?birthName)    = "nl" || LANG(?birthName)    = "")
  FILTER(!BOUND(?Place__label) || LANG(?Place__label) = "nl" || LANG(?Place__label) = "")
}
```

### 15.4 Entity count with breakout

Count Persons by birthPlace label — `COUNT(DISTINCT ?subject)` so multi-valued
birthplaces do not inflate the count:

```sparql
SELECT ?Place__label (COUNT(DISTINCT ?subject) AS ?ag_0)
WHERE {
  ?subject a <http://dbpedia.org/ontology/Person> .
  OPTIONAL { ?subject       <http://dbpedia.org/ontology/birthPlace>    ?Place_subject . }
  OPTIONAL { ?Place_subject <http://www.w3.org/2000/01/rdf-schema#label> ?Place__label . }
  FILTER(!BOUND(?Place__label) || LANG(?Place__label) = "nl" || LANG(?Place__label) = "")
}
GROUP BY ?Place__label
ORDER BY DESC(?ag_0)
```

---

## 16. Invariants checklist

When changing the transpiler, the compiled output should still satisfy:

1. **Row variable** is `?subject`; join vars follow `?<alias>_subject` /
   `?<alias>__<field>`; the subject is detected by the reserved name, never by
   `:type/PK`.
2. **All IRIs are inline `<…>`**; no `PREFIX` block in generated output; no
   double-bracketed parameter values.
3. **Class triple is first**; selective equality constants are pushed into
   mandatory triples, not left in a trailing FILTER (except inside `:or`,
   langString, and geometry columns).
4. **FK chains are flat-paired sibling OPTIONALs**, each hop anchored on the
   previous intermediate var — never re-anchored on `?subject`, never nested.
5. **No duplicate triple** binding the same predicate-on-same-subject to two
   variables (`var-for-token` reuses one variable per path).
6. **Every langString variable** has its `!BOUND || LANG = "x" || LANG = ""`
   guard, placed after the OPTIONALs and before residual user FILTERs.
7. **Geometry equality uses `STR()`**; geometry/coordinate columns are not
   pushed into anchors.
8. **Arg-less entity counts** use `COUNT(DISTINCT ?subject)` in a base stage
   (`COUNT(*)` only in a derived stage with no `?subject`).
9. **Column count and order** match Lib's `expected-cols`; unresolved columns
   become `?undefined_N`, never silently dropped.
10. **Native parameters** are rendered by type with the placeholder left bare;
    multi-value tags belong inside `IN(...)` / `VALUES`.
