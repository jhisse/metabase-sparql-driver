(ns metabase.driver.sparql.mbql
  "MBQL → SPARQL compilation: projection, filters, custom expressions, ordering,
   implicit and explicit left joins, aggregations with temporal breakouts, and
   multi-stage (sub-SELECT) queries."
  (:require
   [clojure.string :as str]
   [clojure.set :as set]
   [metabase.driver-api.core :as driver-api]
   [metabase.driver.sparql.uri :as uri]
   [metabase.lib.metadata.result-metadata :as result-metadata]
   [metabase.util.date-2 :as u.date]
   [metabase.util.log :as log])
  (:import
   (java.time LocalDate LocalDateTime OffsetDateTime ZoneId ZonedDateTime)
   (java.time.format DateTimeFormatter)))

(defn- sanitize-var-name
  "Return `s` as a SPARQL variable name: characters outside `[A-Za-z0-9_]`
   become `_`, a leading digit gets a `_` prefix, and a blank result is `v`."
  [s]
  (let [base (-> (str s)
                 (str/replace #"[^A-Za-z0-9_]" "_")
                 (str/replace #"^([0-9])" "_$1"))]
    (if (str/blank? base) "v" base)))

(defn- field-token->id
  "Return the id of a `[:field id opts]` token (an integer, or a column-name
   string in a derived stage), or nil for any other value."
  [field-token]
  (when (and (vector? field-token)
             (= :field (first field-token)))
    (second field-token)))

(defn- field-token->opts
  "Return the options map of a `[:field id opts]` token, or nil."
  [field-token]
  (when (and (vector? field-token) (= :field (first field-token)))
    (let [opts (nth field-token 2 nil)]
      (when (map? opts) opts))))

(defn- field-token->join-alias
  "Return the `:join-alias` of a field token, or nil. A field with one is read
   off the entity of a join (implicit or explicit), not off the row itself."
  [field-token]
  (:join-alias (field-token->opts field-token)))

(declare collect-expression-tokens)

(defn- expression-token?
  "True for an `[:expression \"name\" opts?]` reference (a custom column)."
  [token]
  (and (vector? token) (= :expression (first token))))

(defn- expression-token->name
  "Return the expression name referenced by an `[:expression \"name\"]` token,
   or nil for any other token."
  [token]
  (when (expression-token? token) (second token)))

(defn- field-id->metadata
  "Resolve field metadata by numeric ID. Returns nil for non-numeric field refs
   (e.g. string column names produced by nested-query / aggregation outputs) so
   they never reach the application DB as a malformed `field.id` lookup."
  [field-id]
  (when (integer? field-id)
    (driver-api/field (driver-api/metadata-provider) field-id)))

(defn- id-field?
  "True when `field-id` is the synthetic subject column.

   The only reliable signal is the reserved name `subject` that
   `build-pk-field` hardcodes. We must NOT use `:semantic-type :type/PK`:
   Metabase's name-based classifier auto-stamps `:type/PK` on *any* field
   literally named `id`, and after Default-Graph URI shortening a real RDF
   property `<base>/id` becomes a column named `id`. Treating it as the
   subject would silently drop it from the SELECT."
  [field-id]
  (= "subject" (:name (field-id->metadata field-id))))

(defn- database-naming-context
  "Return the URI-naming context (Default Graph + namespace prefixes) of the
   current database, which expands shortened table and field names back to
   full URIs."
  []
  (uri/naming-context (some-> (driver-api/database (driver-api/metadata-provider))
                              :details)))

(defn- database-default-language
  "Return the Default Language (BCP-47 tag) from the connection details; nil or
   blank when unset."
  []
  (some-> (driver-api/database (driver-api/metadata-provider))
          :details
          :default-language))

(defn- lang-string-field?
  "True when the field's `:database-type` indicates an `rdf:langString` property
   (set during SHACL sync). Other sync strategies never emit `\"langString\"`,
   so this is effectively a SHACL-only signal."
  [field-id]
  (= "langString" (:database-type (field-id->metadata field-id))))

(defn- lang-filter
  "Render the LANG filter for one variable, accepting untagged literals
   alongside the target language. LANGMATCHES ignores case and accepts
   subtags, so `en` also keeps `@en-GB`. It goes inside the OPTIONAL that binds
   the variable, so an entity without a value in that language keeps its row
   with the value unbound, instead of losing the row."
  [var-name lang]
  (format "FILTER(LANGMATCHES(LANG(?%s), %s) || LANG(?%s) = \"\")"
          var-name (uri/string-literal lang) var-name))

(defn- table-id->class-uri
  "Return the RDF class URI of table `table-id`: its name, expanded to a full
   URI through [[database-naming-context]]. Throws when it resolves to nothing,
   which `?subject a <>` would silently match no rows for."
  [table-id]
  (let [nm  (some->> table-id (driver-api/table (driver-api/metadata-provider)) :name)
        uri (uri/absolute-uri nm (database-naming-context))]
    (log/debugf "[mbql] Resolved class URI for table-id %s: %s" table-id uri)
    (when (str/blank? uri)
      (throw (ex-info (str "The SPARQL driver cannot resolve the class of table " (pr-str table-id))
                      {:type driver-api/qp.error-type.driver})))
    uri))

(defn- collect-field-ids
  "Return a vector of the distinct ids of fields read off the row itself in the
   stage's `:fields`, `:order-by`, `:filter` and `:expressions`. A `:join-alias`
   token is left out: it reads the joined entity, through `pair->target-var`."
  [{:keys [fields order-by expressions] filter-clause :filter}]
  (let [direct-ids      #(set (keep field-token->id (remove field-token->join-alias %)))
        ids-from-fields (direct-ids fields)
        ids-from-order  (direct-ids (map second order-by))
        ids-from-filter (letfn [(collect-field-tokens [x]
                                  (cond
                                    (and (vector? x) (= :field (first x))) [x]
                                    (sequential? x) (mapcat collect-field-tokens x)
                                    (map? x) (mapcat collect-field-tokens (vals x))
                                    :else []))]
                          (direct-ids (collect-field-tokens filter-clause)))
        ids-from-expr   (direct-ids (collect-expression-tokens expressions))
        all-ids         (vec (set/union ids-from-fields ids-from-order ids-from-filter ids-from-expr))]
    (log/debugf "[mbql] Collected field IDs: fields=%d order=%d filter=%d total=%d"
                (count ids-from-fields) (count ids-from-order) (count ids-from-filter) (count all-ids))
    all-ids))

(defn- var-for-token
  "Return the SPARQL variable name (without `?`) for a `[:field id opts]` or
   `[:expression \"name\"]` token, or nil when it cannot be resolved.

   - An expression token maps to its sanitized name (the custom column's BIND).
   - If the token carries `:join-alias`, look up the joined target var via
     `pair->target-var` keyed by `[field-id alias source-field]` (a FK column's
     display value inside an explicit join), else by `[field-id alias]`.
   - Otherwise the subject (PK) field always maps to `?subject`.
   - Otherwise look up the regular alias from `field-id->var`. A string ref
     (a source-query column name, e.g. `birth-date`) also tries its sanitized
     form, since derived stages key their columns by SPARQL var name."
  [field-token field-id->var pair->target-var]
  (let [fid   (field-token->id field-token)
        alias (field-token->join-alias field-token)]
    (cond
      (expression-token? field-token) (sanitize-var-name (expression-token->name field-token))
      (and fid alias) (or (get pair->target-var [fid alias (:source-field (field-token->opts field-token))])
                          (get pair->target-var [fid alias]))
      (and fid (id-field? fid)) "subject"
      fid (or (get field-id->var fid)
              (when (string? fid)
                (get field-id->var (sanitize-var-name fid)))))))

(defn- condition->fk-ref
  "Return the FK-source field token of a join `:condition`, or nil. The condition is
   a legacy-MBQL filter clause, normally `[:= <fk> <target>]` (or wrapped in
   `[:and …]`, in which case we unwrap to the first `:=`).

   - In a single-hop join the FK side has no `:join-alias` and the target side
     does, so picking the non-join-alias token works.
   - In a chained explicit join (e.g. Item → Provider → Owner from the notebook
     editor), BOTH sides carry a `:join-alias`. When `this-alias` is provided,
     we pick the side whose alias is NOT this join's own alias — that side
     refers to a previously-joined table and is the real FK source."
  ([condition] (condition->fk-ref condition nil))
  ([condition this-alias]
   (when (sequential? condition)
     (when-let [eq (condp = (first condition)
                     := condition
                     :and (first (filter #(and (sequential? %) (= := (first %)))
                                         (rest condition)))
                     nil)]
       (let [fields (->> (rest eq) (filter #(and (vector? %) (= :field (first %)))))]
         (or (->> fields (remove field-token->join-alias) first)
             (when this-alias
               (->> fields
                    (remove #(= this-alias (field-token->join-alias %)))
                    first))))))))

(defn- condition->fk-field-id
  "Return the field id of the FK-source side of a join `:condition` (see [[condition->fk-ref]])."
  ([condition] (field-token->id (condition->fk-ref condition)))
  ([condition this-alias] (field-token->id (condition->fk-ref condition this-alias))))

(defn- collect-joined-pairs
  "Walk a legacy-MBQL stage and return the set of `[field-id alias source-field]`
   for every `[:field id {:join-alias \"...\"}]` token that appears in `:fields`,
   `:order-by`, `:expressions`, or `:filter`. `source-field` is the token's
   `:source-field`, or nil."
  [{:keys [fields order-by expressions] filter-clause :filter}]
  (letfn [(walk [x]
            (cond
              (and (vector? x) (= :field (first x)))
              (when-let [a (field-token->join-alias x)]
                [[(field-token->id x) a (:source-field (field-token->opts x))]])
              (sequential? x) (mapcat walk x)
              (map? x) (mapcat walk (vals x))
              :else []))]
    (set (concat (mapcat walk (or fields []))
                 (mapcat walk (mapcat (fn [[_dir fld & _]] [fld]) (or order-by [])))
                 (mapcat walk (collect-expression-tokens expressions))
                 (walk filter-clause)))))

(def ^:private null-term
  "SPARQL expression that stands in for null, which SPARQL has no literal for:
   `?0null`, a variable nothing binds. Its name starts with a digit, which
   [[sanitize-var-name]] never produces, so no column can bind it. Evaluating
   it is an expression error, which leaves a BIND unbound, makes IF unbound,
   is skipped by COALESCE and behaves like SQL's UNKNOWN under `&&`, `||` and
   `!`, on every engine tested (Oxigraph, Jena, RDF4J, Virtuoso). `(1/0)`
   fails the whole query on RDF4J and Virtuoso instead. Parenthesized, so it
   also stands alone after FILTER."
  ;; Virtuoso 8 (DBpedia) drops the row for `!(false && unbound)`, where SQL
  ;; keeps it; it does so for any unbound variable, not only this one.
  "(?0null)")

(defn- literal->sparql
  "Render `v` as a SPARQL literal: numbers and booleans bare, anything else as
   a string literal via [[uri/string-literal]]. Throws on nil, which has no
   literal: a filter against a missing value compiles to [[null-term]]
   before reaching here."
  [v]
  (cond
    (string? v) (uri/string-literal v)
    (number? v) (str v)
    (boolean? v) (if v "true" "false")
    (nil? v) (throw (ex-info "A missing value has no SPARQL literal." {}))
    :else (uri/string-literal v)))

(defn- iri-value?
  "True when the filter value `v` on `field-id` is an IRI: a scheme-carrying
   value on an IRI-valued column (see [[value->term]])."
  [field-id v]
  (and (uri/has-scheme? v)
       (let [meta (field-id->metadata field-id)]
         (or (= :type/FK (:semantic-type meta))
             (= "uri" (:database-type meta))))))

(defn- value->term
  "Render `v`, the right-hand value of an `:=`/`:!=` filter on `field-id`, as a
   SPARQL term.

   An IRI-valued field — a foreign key (`:semantic-type :type/FK`) or a
   column whose values are IRI nodes (`:database-type \"uri\"`: the synthetic
   subject, auto-sync-discovered IRI properties, SHACL `sh:nodeKind sh:IRI`) —
   is bound to IRI *nodes*, so a scheme-carrying value must be emitted as
   `<iri>` (via [[uri/iri-ref]], which escapes it) or the comparison can never
   match. Any scheme counts ([[uri/has-scheme?]]: `did:`, `HTTPS://`, …),
   which is safe because the field metadata already says the values are IRIs.
   Everything else falls back to [[literal->sparql]]; notably `:type/URL`
   columns stay literals, because they hold `xsd:anyURI`-typed literal values,
   not IRI nodes.

   Range filters keep literals: ordering comparisons on IRI terms are a SPARQL
   type error."
  ;; In a derived stage field refs are column-name strings, so
  ;; `field-id->metadata` returns nil and the value stays a literal.
  [field-id v]
  (if (iri-value? field-id v)
    (uri/iri-ref v)
    (literal->sparql v)))

(defn- unsupported-filter!
  "Throw for a filter the compiler cannot translate. Returning nil instead
   would drop the filter and silently return unfiltered rows."
  [what filter-clause]
  (throw (ex-info (format "The SPARQL driver does not support %s in filters." what)
                  {:type   driver-api/qp.error-type.unsupported-feature
                   :clause filter-clause})))

(def ^:private xsd-date     "<http://www.w3.org/2001/XMLSchema#date>")
(def ^:private xsd-datetime "<http://www.w3.org/2001/XMLSchema#dateTime>")
(def ^:private xsd-integer  "<http://www.w3.org/2001/XMLSchema#integer>")
(def ^:private xsd-double   "<http://www.w3.org/2001/XMLSchema#double>")
(def ^:private xsd-decimal  "<http://www.w3.org/2001/XMLSchema#decimal>")

(def ^:private ^DateTimeFormatter xsd-datetime-format
  "xsd:dateTime lexical form. Seconds are always written: ISO_OFFSET_DATE_TIME
   drops them when zero, which xsd:dateTime does not allow."
  (DateTimeFormatter/ofPattern "uuuu-MM-dd'T'HH:mm:ssXXX"))

(defn- query-zone
  "Return the zone that relative dates and date-only values are resolved in (the QP's
   results timezone)."
  ^ZoneId []
  (ZoneId/of (driver-api/results-timezone-id)))

(defn- query-now
  "Return the current time in [[query-zone]]; the reference point for `:relative-datetime`."
  ^ZonedDateTime []
  (ZonedDateTime/now (query-zone)))

(defn- temporal-clause?
  [x]
  (and (vector? x) (contains? #{:absolute-datetime :relative-datetime} (first x))))

(defn- temporal-clause->value
  "Resolve `[:absolute-datetime t unit]` / `[:relative-datetime n unit]` to a
   java.time value. A relative datetime is the start of `unit`, `n` units from
   now (the same semantics as the SQL and Mongo drivers); `:current` or a
   missing unit gives the current time itself, untruncated."
  [[tag a b]]
  (case tag
    :absolute-datetime (if (string? a) (u.date/parse a) a)
    :relative-datetime (let [now (query-now)]
                         (if (or (= a :current) (contains? #{nil :default} b))
                           now
                           (u.date/truncate (u.date/add now b a) b)))))

(def ^:private ^DateTimeFormatter xsd-local-datetime-format
  (DateTimeFormatter/ofPattern "uuuu-MM-dd'T'HH:mm:ss"))

(defn- temporal->sparql
  "Render a java.time value as a typed literal matching the column: xsd:date
   for `:type/Date` columns, xsd:dateTime otherwise. The column type comes from
   field metadata or, for a source-query column ref, the ref's own options.

   An xsd:dateTime bound is returned as `{:tz … :local …}`: the same instant
   with and without a timezone offset. XSD comparisons between a value with a
   timezone and one without are indeterminate, so [[compare-expr]] picks the
   bound matching each value's `TZ()`."
  [field-id opts t]
  (let [base-type (or (:base-type (field-id->metadata field-id))
                      (:effective-type opts)
                      (:base-type opts))
        date?     (if base-type
                    (isa? base-type :type/Date)
                    (instance? LocalDate t))]
    (if date?
      (str "\"" (condp instance? t
                  LocalDate      t
                  LocalDateTime  (.toLocalDate ^LocalDateTime t)
                  OffsetDateTime (.toLocalDate ^OffsetDateTime t)
                  ZonedDateTime  (.toLocalDate (.withZoneSameInstant ^ZonedDateTime t (query-zone))))
           "\"^^" xsd-date)
      (let [^ZonedDateTime zdt (condp instance? t
                                 LocalDate      (.atStartOfDay ^LocalDate t (query-zone))
                                 LocalDateTime  (.atZone ^LocalDateTime t (query-zone))
                                 OffsetDateTime (.atZoneSameInstant ^OffsetDateTime t (query-zone))
                                 ZonedDateTime  (.withZoneSameInstant ^ZonedDateTime t (query-zone)))]
        {:tz    (str "\"" (.format xsd-datetime-format zdt) "\"^^" xsd-datetime)
         :local (str "\"" (.format xsd-local-datetime-format zdt) "\"^^" xsd-datetime)}))))

(defn- compare-expr
  "Render the comparison `?var op term`. For a `{:tz … :local …}` dateTime bound, compare each value
   against the form that matches whether it carries a timezone."
  [var op term]
  (if (map? term)
    (format "((TZ(?%s) != \"\" && ?%s %s %s) || (TZ(?%s) = \"\" && ?%s %s %s))"
            var var op (:tz term) var var op (:local term))
    (format "?%s %s %s" var op term)))

(defn- unwrap-value
  "Return `x`, or the value inside a `[:value x …]` wrapper."
  [x]
  (if (and (vector? x) (= :value (first x)))
    (second x)
    x))

(def ^:private comparison-ops
  "SPARQL operator of each MBQL ordering comparison."
  {:> ">" :>= ">=" :< "<" :<= "<="})

(def ^:private string-match-fns
  "SPARQL function of each MBQL string match."
  {:starts-with "STRSTARTS" :ends-with "STRENDS" :contains "CONTAINS"})

(defn- negated-leaf-var
  "Return the variable of the string match `clause` when negating it must keep
   the rows where that variable is unbound, or nil. The match fails on an
   unbound variable, and `!` of that failure drops the row, while the SQL
   drivers keep it: \"does not contain\" keeps NULLs. As there, only string
   matches get this; `NOT (x > 5)` still drops rows without a value."
  [clause field-id->var pair->target-var]
  (let [[op lhs rhs] (when (sequential? clause) clause)]
    (when (and (contains? #{:starts-with :ends-with :contains} op)
               (some? (unwrap-value rhs)))
      (var-for-token lhs field-id->var pair->target-var))))

(defn- equality-expr
  "Render the equality of `?var` and the filter value `v` on `fid`, with `term`
   the value rendered by [[value->term]]. A string that is not an IRI compares
   the text: `\"Paris\"@en = \"Paris\"` and `\"x\"^^xsd:anyURI = \"x\"` are false or
   a type error in SPARQL, and a value picked from a filter list carries no
   tag or datatype. `STR(?v) = \"x\"` never raises a type error on a bound
   variable, so its negation keeps every row that does not match."
  [var fid v term]
  (if (and (string? v) (not (iri-value? fid v)))
    (format "STR(?%s) = %s" var term)
    (compare-expr var "=" term)))

(defn- compile-filter-expr
  "Compile a filter clause to a SPARQL boolean expression string, or nil for a
   non-clause or an empty `:and`/`:or`. Throws (via [[unsupported-filter!]])
   for operators, functions, date groupings or column references it cannot
   translate."
  [filter-clause field-id->var pair->target-var]
  (when (sequential? filter-clause)
    (let [[op lhs rhs maybe-opts] filter-clause]
      (case op
        :and (let [parts (->> (rest filter-clause)
                              (keep #(compile-filter-expr % field-id->var pair->target-var)))]
               (when (seq parts)
                 (str "(" (str/join " && " parts) ")")))
        :or  (let [parts (->> (rest filter-clause)
                              (keep #(compile-filter-expr % field-id->var pair->target-var)))]
               (when (seq parts)
                 (str "(" (str/join " || " parts) ")")))
        :not (when-let [inner (compile-filter-expr lhs field-id->var pair->target-var)]
               (if-let [unbound-var (negated-leaf-var lhs field-id->var pair->target-var)]
                 (format "(!BOUND(?%s) || !%s)" unbound-var inner)
                 (str "(!" inner ")")))
        (let [fid (field-token->id lhs)
              var (var-for-token lhs field-id->var pair->target-var)
              opts (when (map? maybe-opts) maybe-opts)
              insensitive? (false? (:case-sensitive opts))
              v (unwrap-value rhs)]
          ;; A field LHS carries `fid`; an `[:expression …]` LHS has none but still
          ;; resolves to a `var` (the custom-column BIND) and must compile.
          (when-not (or fid (expression-token? lhs))
            (unsupported-filter! (if (and (vector? lhs) (keyword? (first lhs)))
                                   (str "the " (name (first lhs)) "() function")
                                   "this expression")
                                 filter-clause))
          (when-not var
            (unsupported-filter! (str "a reference to an unresolved column (" (pr-str lhs) ")")
                                 filter-clause))
          (let [unit        (:temporal-unit (field-token->opts lhs))
                null-check? (or (contains? #{:is-null :not-null} op)
                                (and (contains? #{:= :!=} op) (nil? v)))]
            ;; A null check does not depend on the grouping, so it stays valid.
            (when (and unit (not= unit :default) (not null-check?))
              (unsupported-filter! (str "a date grouped by " (name unit)) filter-clause)))
          (let [term (fn [x render]
                       (cond
                         (temporal-clause? x) (temporal->sparql fid (field-token->opts lhs) (temporal-clause->value x))
                         (vector? x)          (unsupported-filter! (str "comparing against a " (name (first x)) " clause")
                                                                   filter-clause)
                         :else                (render x)))]
            (case op
              := (if (nil? v)
                   (format "(!BOUND(?%s))" var)
                   (str "(" (equality-expr var fid v (term v #(value->term fid %))) ")"))
              ;; A row without the value is "not equal" too, as in the SQL drivers.
              :!= (if (nil? v)
                    (format "(BOUND(?%s))" var)
                    (format "(!BOUND(?%s) || !(%s))" var (equality-expr var fid v (term v #(value->term fid %)))))
              ;; Comparing with a missing value is unknown, as `x > NULL` is in
              ;; SQL. The expression error of `null-term` behaves like UNKNOWN
              ;; under `&&`, `||` and `!`, and a FILTER on it matches nothing; a
              ;; nil would drop the FILTER and return every row.
              (:> :>= :< :<=)
              (if (nil? v)
                null-term
                (str "(" (compare-expr var (comparison-ops op) (term v literal->sparql)) ")"))
              ;; [:between field min max] — min is rhs (`v`), max is the next arg.
              :between (let [hi (unwrap-value maybe-opts)]
                         (if (or (nil? v) (nil? hi))
                           null-term
                           (str "(" (compare-expr var ">=" (term v literal->sparql))
                                " && " (compare-expr var "<=" (term hi literal->sparql)) ")")))
              (:starts-with :ends-with :contains)
              (if (nil? v)
                null-term
                (let [needle (let [t (term v literal->sparql)] (if (map? t) (:tz t) t))
                      fname  (string-match-fns op)]
                  (str "(" (if insensitive?
                             (format "%s(LCASE(STR(?%s)), LCASE(%s))" fname var needle)
                             (format "%s(STR(?%s), %s)" fname var needle))
                       ")")))
              :is-null (format "(!BOUND(?%s))" var)
              :not-null (format "(BOUND(?%s))" var)
              (unsupported-filter! (str "the " (name op) " operator") filter-clause))))))))

(defn- build-var-aliases
  "Return a map from each of `field-ids` to its SPARQL variable: the sanitized
   field name, or `f_<id>` when the field has no metadata."
  [field-ids]
  (let [aliases (into {}
                      (for [fid field-ids
                            :let [meta (field-id->metadata fid)
                                  nm   (or (:name meta) (str "f_" fid))]]
                        [fid (sanitize-var-name nm)]))]
    (log/debugf "[mbql] Built %d var aliases" (count aliases))
    aliases))

(defn- triple-pattern
  "Render `?source <property> ?target .`."
  [source-var property-uri target-var]
  (format "?%s %s ?%s ." source-var (uri/iri-ref property-uri) target-var))

(defn- emit-optional-group
  "Render a SPARQL `OPTIONAL { <pattern> <pattern> … }` line."
  [patterns]
  (str "  OPTIONAL { " (str/join " " patterns) " }"))

(defn- emit-remap-optional
  "Render the OPTIONAL that reads `property` off `fk-var`, a variable bound by a
   sub-SELECT. `fk-var` is unbound on rows without the FK, and a plain
   `OPTIONAL { ?fk-var <p> ?t }` (or one guarded by `BOUND(?fk-var)`) would then
   bind it to every node with `<p>`. Matching a fresh var and comparing it with
   `=` does not: the comparison errors on the unbound row, so the row is kept
   without a value."
  ;; The fresh var scans every `<p>` triple; fine for remaps, revisit
  ;; if a derived stage ever remaps over a very large property.
  [fk-var property-uri target-var]
  (let [node (str fk-var "_node")]
    (emit-optional-group [(triple-pattern node property-uri target-var)
                          (format "FILTER(?%s = ?%s)" node fk-var)])))

(defn- joined-var-name
  "Build a SPARQL var name for a joined column: `<alias>__<field-name>`,
   sanitized. `field-name` may be nil; falls back to `f`."
  [alias field-name]
  (sanitize-var-name (str alias "__" (or field-name "f"))))

(defn- ensure-triple-for-field
  "Return the OPTIONAL line that binds `?var-alias` to `property-uri` of
   `?subject`, with the [[lang-filter]] `lang-filter` (or nil) inside it."
  [property-uri var-alias lang-filter]
  (let [triple (emit-optional-group (cond-> [(triple-pattern "subject" property-uri var-alias)]
                                      lang-filter (conj lang-filter)))]
    (log/debugf "[mbql] OPTIONAL triple: property=%s var=?%s" property-uri var-alias)
    triple))

(defn- compile-basic-filter
  "Compile a filter clause to a vector holding a single FILTER line, or nil."
  [filter-clause field-id->var pair->target-var]
  (when-let [expr (compile-filter-expr filter-clause field-id->var pair->target-var)]
    [(str "  FILTER " expr)]))

;; ---------------------------------------------------------------------------
;; Custom expressions (Metabase "custom columns") → SPARQL
;; ---------------------------------------------------------------------------

(defn- regex-escape
  "Escape regex metacharacters so `s` matches literally inside a SPARQL REPLACE pattern."
  [s]
  (str/replace (str s) #"([\\.^$|?*+()\[\]{}])" "\\\\$1"))

(declare compile-expression)

(defn- expr-arg
  "Compile one argument of an expression to a SPARQL expression string: a literal,
   a `[:value v]` wrapper, a `[:field …]`/`[:expression …]` token (→ `?var`), or a
   nested operation. nil compiles to [[null-term]]. Tokens resolve through
   [[var-for-token]]; throws when one does not resolve."
  [arg field-id->var pair->target-var]
  (cond
    (number? arg)  (str arg)
    (string? arg)  (uri/string-literal arg)
    (boolean? arg) (if arg "true" "false")
    (nil? arg)     null-term
    (and (vector? arg) (= :value (first arg)))      (expr-arg (second arg) field-id->var pair->target-var)
    (and (vector? arg) (#{:field :expression} (first arg)))
    (if-let [v (var-for-token arg field-id->var pair->target-var)]
      (str "?" v)
      (throw (ex-info "Cannot resolve field/expression reference in expression"
                      {:token arg})))
    (sequential? arg) (compile-expression arg field-id->var pair->target-var)
    :else (throw (ex-info "Unsupported expression argument" {:arg arg}))))

(defn- compile-case
  "Compile a `[:case [[pred val]…] {:default d}]` clause to nested SPARQL `IF()`.
   A predicate is a filter clause and compiles like one; a bare boolean column
   ref is used as is. Without a `:default`, unmatched rows get [[null-term]].

   Each predicate is wrapped in `COALESCE(…, false)`: on a row where it reads
   a missing value it is an error, which would make the whole IF unbound,
   while SQL takes an UNKNOWN WHEN as false and tries the next one."
  [args field-id->var pair->target-var]
  (let [clauses (first args)
        opts    (second args)
        default (when (map? opts) (:default opts))
        a       #(expr-arg % field-id->var pair->target-var)
        pred    #(if (and (vector? %) (#{:field :expression} (first %)))
                   (a %)
                   (compile-filter-expr % field-id->var pair->target-var))]
    (reduce (fn [else [p val]]
              (format "IF(COALESCE(%s, false), %s, %s)" (pred p) (a val) else))
            (if (some? default) (a default) null-term)
            (reverse clauses))))

(defn- compile-expression
  "Compile a Metabase expression clause to a SPARQL expression string. Covers
   arithmetic, rounding, string functions, `regexextract` with a literal
   pattern, `case`, `coalesce` and casts. Throws `ex-info` on an unsupported
   function so the query fails with a clear message rather than silently
   dropping the column."
  [clause field-id->var pair->target-var]
  (let [a #(expr-arg % field-id->var pair->target-var)
        s #(format "STR(%s)" (a %))
        cast (fn [iri x] (format "%s(%s)" iri (a x)))]
    (if-not (sequential? clause)
      (a clause)
      (let [[op & args] clause]
        (case op
          :+ (str "(" (str/join " + " (map a args)) ")")
          :- (if (= 1 (count args))
               (str "(- " (a (first args)) ")")
               (str "(" (str/join " - " (map a args)) ")"))
          :* (str "(" (str/join " * " (map a args)) ")")
          ;; `+ 0.0` makes an integer numerator a decimal: SPARQL divides
          ;; integers as decimals, but Virtuoso truncates (30 / 7 = 4). Each
          ;; quotient is then a decimal already. A zero divisor gives null, as
          ;; in the SQL drivers; Virtuoso would fail the whole query on it. A
          ;; literal divisor is settled here; any other is checked per row.
          ;; That writes the divisor twice, so a division nested inside a
          ;; divisor doubles the text per level; fine for hand-written
          ;; expressions, a BIND per divisor if it ever is not.
          :/ (reduce (fn [acc divisor]
                       (let [lit (unwrap-value divisor)
                             d   (a divisor)]
                         (cond
                           (and (number? lit) (zero? lit)) null-term
                           (number? lit) (format "(%s / %s)" acc d)
                           :else (format "IF(%s = 0, %s, (%s / %s))" d null-term acc d))))
                     (format "(%s + 0.0)" (a (first args)))
                     (rest args))
          :abs   (format "ABS(%s)" (a (first args)))
          :ceil  (format "CEIL(%s)" (a (first args)))
          :floor (format "FLOOR(%s)" (a (first args)))
          :round (format "ROUND(%s)" (a (first args)))
          :length (format "STRLEN(%s)" (s (first args)))
          :lower  (format "LCASE(%s)" (s (first args)))
          :upper  (format "UCASE(%s)" (s (first args)))
          :trim   (format "REPLACE(%s, \"^\\\\s+|\\\\s+$\", \"\")" (s (first args)))
          :ltrim  (format "REPLACE(%s, \"^\\\\s+\", \"\")" (s (first args)))
          :rtrim  (format "REPLACE(%s, \"\\\\s+$\", \"\")" (s (first args)))
          :concat (format "CONCAT(%s)" (str/join ", " (map s args)))
          :coalesce (format "COALESCE(%s)" (str/join ", " (map a args)))
          :substring (let [[txt start len] args]
                       (if (some? len)
                         (format "SUBSTR(%s, %s, %s)" (s txt) (a start) (a len))
                         (format "SUBSTR(%s, %s)" (s txt) (a start))))
          :replace (let [[txt find repl] args
                         find-str (if (string? find) find (second find))
                         repl-str (if (string? repl) repl (second repl))]
                     ;; `\` and `$` are special in a REPLACE replacement string.
                     (format "REPLACE(%s, %s, %s)"
                             (s txt)
                             (uri/string-literal (regex-escape find-str))
                             (uri/string-literal (str/replace (str repl-str) #"[\\$]" "\\\\$0"))))
          :regex-match-first
          ;; REPLACE returns its input unchanged when nothing matches, so a
          ;; non-matching row gets null from the REGEX guard instead. "s" lets
          ;; `.` cross newlines.
          (let [[txt pat] args
                pat-str (cond
                          (string? pat) pat
                          (and (vector? pat) (= :value (first pat)) (string? (second pat))) (second pat)
                          :else (throw (ex-info (str "regexextract needs a literal pattern; the SPARQL driver "
                                                     "cannot use a column or expression as the regex.")
                                                {:type driver-api/qp.error-type.unsupported-feature
                                                 :clause clause})))]
            (format "IF(REGEX(%s, %s, \"s\"), REPLACE(%s, %s, \"$1\", \"s\"), %s)"
                    (s txt) (uri/string-literal pat-str)
                    (s txt) (uri/string-literal (str "^.*?(" pat-str ").*$"))
                    null-term))
          :float   (cast xsd-double (first args))
          ;; Metabase rounds; the xsd:integer constructor truncates. Decimal
          ;; keeps long integers exact, where double would not.
          :integer (format "%s(ROUND(%s(%s)))" xsd-integer xsd-decimal (a (first args)))
          :text    (format "STR(%s)" (a (first args)))
          :case    (compile-case args field-id->var pair->target-var)
          (throw (ex-info (str "Unsupported expression function: " op)
                          {:type driver-api/qp.error-type.unsupported-feature
                           :op op :clause clause})))))))

(defn- collect-expression-tokens
  "Collect every `[:field …]`/`[:expression …]` token appearing inside the values
   of an `:expressions` map, so fields referenced only by a custom column still
   get their triples emitted."
  [expressions]
  (letfn [(walk [x]
            (cond
              (and (vector? x) (#{:field :expression} (first x))) [x]
              (sequential? x) (mapcat walk x)
              (map? x) (mapcat walk (vals x))
              :else []))]
    (mapcat walk (vals (or expressions {})))))

(defn- compile-expressions
  "Compile a stage's `:expressions` map to a vector of `BIND(… AS ?name)` lines,
   one per expression. An expression that references another comes after it:
   the legacy `:expressions` map loses Lib's order past 8 entries, and a BIND
   cannot read a variable bound later. Throws on a reference cycle.

   Also throws when a custom column's variable is already `taken` by another
   column, or shared by two custom columns whose names differ only in
   characters [[sanitize-var-name]] replaces (`my-col` and `my col`): a BIND
   onto a bound variable is a SPARQL error."
  [expressions field-id->var pair->target-var taken]
  (doseq [[v names] (group-by sanitize-var-name (keys expressions))
          :when (or (taken v) (next names))]
    (throw (ex-info (format "Rename the custom column %s: its SPARQL variable ?%s is already used by another column."
                            (pr-str (first names)) v)
                    {:type driver-api/qp.error-type.unsupported-feature})))
  (loop [todo (into (sorted-map) expressions) done #{} lines []]
    (if (empty? todo)
      lines
      (let [ready? (fn [[_ clause]]
                     (every? done (keep expression-token->name (collect-expression-tokens {nil clause}))))
            [ename clause] (or (first (filter ready? todo))
                               (throw (ex-info "Custom columns reference each other in a cycle"
                                               {:expressions (keys todo)})))]
        (recur (dissoc todo ename)
               (conj done ename)
               (conj lines (str "  BIND(" (compile-expression clause field-id->var pair->target-var)
                                " AS ?" (sanitize-var-name ename) ")")))))))

(defn- compile-order-by
  "Compile a non-aggregation `order-by` to an `ORDER BY` clause string, or nil.
   Terms whose token does not resolve to a variable are skipped."
  [order-by field-id->var pair->target-var]
  (when (seq order-by)
    (let [parts (for [[dir fld & _] order-by
                      :let [var (var-for-token fld field-id->var pair->target-var)]]
                  (when var
                    (str (str/upper-case (name dir)) "(?" var ")")))
          parts (remove nil? parts)]
      (when (seq parts)
        (let [clause (str "ORDER BY " (str/join " " parts))]
          (log/debugf "[mbql] Order clause: %s" clause)
          clause)))))

(defn- unwrap-aggregation
  "Strip an `:aggregation-options` wrapper, returning the inner aggregation clause."
  [agg]
  (if (and (sequential? agg) (= :aggregation-options (first agg)))
    (second agg)
    agg))

(defn- aggregation-arg-token
  "Return the `[:field …]`/`[:expression …]` token an aggregation operates on, or
   nil for arg-less aggregations such as `[:count]`."
  [agg]
  (let [agg (unwrap-aggregation agg)
        arg (when (sequential? agg) (second agg))]
    (when (and (vector? arg) (#{:field :expression} (first arg)))
      arg)))

(defn- aggregation-output-name
  "Return Metabase's default result-column name for an aggregation clause — the name a
   *later* stage uses to reference the aggregation as a plain field (e.g. drilling
   on a count value adds a filter `[:< [:field \"count\" …] 12]`). An
   `:aggregation-options` `:name` wins; otherwise it is derived from the operator
   (`:distinct` reports as `\"count\"`, matching Metabase). Returns nil for
   operators with no stable default name."
  [agg]
  (or (when (and (sequential? agg) (= :aggregation-options (first agg)))
        (:name (nth agg 2 nil)))
      (let [op (when (sequential? (unwrap-aggregation agg))
                 (first (unwrap-aggregation agg)))]
        (case op
          (:count :cum-count) "count"
          :distinct           "count"
          (:sum :cum-sum)     "sum"
          :avg                "avg"
          :min                "min"
          :max                "max"
          :stddev             "stddev"
          :median             "median"
          nil))))

(defn- aggregation-name->var
  "Map each aggregation in `aggregations` from the name a later stage references
   it by ([[aggregation-output-name]]) to its SPARQL variable (`ag_0`, `ag_1`, …),
   so an outer stage's filter/order-by on an aggregation result resolves. Duplicate
   names are disambiguated `name`, `name_2`, `name_3`, … the way Metabase dedupes."
  [aggregations]
  (let [seen (atom {})]
    (into {}
          (keep-indexed
           (fn [i agg]
             (when-let [nm (aggregation-output-name agg)]
               (let [n   (get (swap! seen update nm (fnil inc 0)) nm)
                     nm* (if (> n 1) (str nm "_" n) nm)]
                 [nm* (str "ag_" i)])))
           aggregations))))

(defn- aggregation->projection
  "Compile one aggregation clause to a SPARQL projection
   `{:select \"(EXPR AS ?ag_N)\" :var \"ag_N\"}`. `token->var` resolves a field
   token to its SPARQL variable. Throws for an aggregation it cannot compile,
   which would otherwise drop the column.

   `[:count]` with no argument becomes `COUNT(DISTINCT ?subject)` in a base
   stage (so the multi-valued OPTIONAL fan-out does not inflate entity counts);
   when `count-all?` is true (derived stage, no `?subject` var) it becomes
   `COUNT(*)`."
  ([agg index token->var]
   (aggregation->projection agg index token->var false))
  ([agg index token->var count-all?]
   (let [agg     (unwrap-aggregation agg)
         op      (when (sequential? agg) (first agg))
         out     (str "ag_" index)
         arg     (aggregation-arg-token agg)
         arg-var (when arg (token->var arg))
         expr    (case op
                   :count    (cond
                               arg-var    (format "COUNT(?%s)" arg-var)
                               count-all? "COUNT(*)"
                               :else      "COUNT(DISTINCT ?subject)")
                   :distinct (when arg-var (format "COUNT(DISTINCT ?%s)" arg-var))
                   :sum      (when arg-var (format "SUM(?%s)" arg-var))
                   :avg      (when arg-var (format "AVG(?%s)" arg-var))
                   :min      (when arg-var (format "MIN(?%s)" arg-var))
                   :max      (when arg-var (format "MAX(?%s)" arg-var))
                   nil)]
     (when-not expr
       (throw (ex-info (format "The SPARQL driver does not support the %s aggregation." (some-> op name))
                       {:type driver-api/qp.error-type.unsupported-feature
                        :clause agg})))
     {:select (format "(%s AS ?%s)" expr out)
      :var    out})))

(def ^:private temporal-bucket-exprs
  "SPARQL expression per breakout `:temporal-unit`, as a format string over the
   value's variable (`%1$s`). Truncations rebuild the value from its lexical form,
   extractions return integers. MONTH()/DAY() on an xsd:date is not in SPARQL 1.1
   but Oxigraph, Jena and RDF4J accept it."
  ;; Buckets follow each value's own lexical timezone, not the report
  ;; timezone; convert first if mixed-timezone data needs report-time buckets.
  (let [quarter-index (str xsd-integer "(FLOOR((MONTH(?%1$s)-1)/3))")]
    {:year            (str "STRDT(CONCAT(SUBSTR(STR(?%1$s),1,4),\"-01-01\"), " xsd-date ")")
     :quarter         (str "STRDT(CONCAT(SUBSTR(STR(?%1$s),1,5), SUBSTR(\"01040710\", "
                           quarter-index "*2+1, 2), \"-01\"), " xsd-date ")")
     :month           (str "STRDT(CONCAT(SUBSTR(STR(?%1$s),1,7),\"-01\"), " xsd-date ")")
     :day             (str "STRDT(SUBSTR(STR(?%1$s),1,10), " xsd-date ")")
     :hour            (str "STRDT(CONCAT(SUBSTR(STR(?%1$s),1,13),\":00:00\"), " xsd-datetime ")")
     :minute          (str "STRDT(CONCAT(SUBSTR(STR(?%1$s),1,16),\":00\"), " xsd-datetime ")")
     :quarter-of-year (str quarter-index "+1")
     :month-of-year   "MONTH(?%1$s)"
     :day-of-month    "DAY(?%1$s)"
     :hour-of-day     "HOURS(?%1$s)"
     :minute-of-hour  "MINUTES(?%1$s)"}))

(defn- bucket-breakout
  "Resolve breakout tokens to the variables to project and group by. A token with
   a `:temporal-unit` groups by a new `?<var>_<unit>` bound to its bucket (see
   [[temporal-bucket-exprs]]). Throws for a unit SPARQL cannot compute (week,
   day-of-week, …) rather than grouping by the raw value.

   Returns `{:vars […] :binds [\"  BIND(…)\" …] :token->var f :aliases {raw bucket}}`,
   where `f` resolves a breakout token (e.g. in `:order-by`) to its bucket var and
   any other token through `token->var`, and `:aliases` lets a later stage find a
   bucket by the raw column name Lib still uses."
  [breakout token->var]
  ;; Keyed by the raw var, not the field id: two paths to one field (a joined column
  ;; and a FK display value inside the same join) resolve to different vars.
  (let [bucket-key (juxt token->var (comp :temporal-unit field-token->opts))
        buckets    (for [tok  breakout
                         :let [raw  (token->var tok)
                               unit (:temporal-unit (field-token->opts tok))]
                         :when raw]
                     (if (contains? #{nil :default} unit)
                       {:tok tok :var raw}
                       (let [expr (or (get temporal-bucket-exprs unit)
                                      (throw (ex-info (format "The SPARQL driver cannot group dates by %s." (name unit))
                                                      {:type  driver-api/qp.error-type.unsupported-feature
                                                       :clause tok})))
                             v    (str raw "_" (sanitize-var-name (name unit)))]
                         {:tok tok :var v :bind (format "  BIND(%s AS ?%s)" (format expr raw) v)})))
        by-key     (into {} (map (juxt (comp bucket-key :tok) :var)) buckets)]
    {:vars       (vec (distinct (map :var buckets)))
     :binds      (vec (distinct (keep :bind buckets)))
     :token->var (fn [tok] (or (get by-key (bucket-key tok)) (token->var tok)))
     :aliases    (into {} (for [{:keys [tok var bind]} buckets :when bind] [(token->var tok) var]))}))

(defn- compile-agg-order-by
  "Compile `order-by` for an aggregation query to an `ORDER BY` clause string,
   or nil. Order terms may reference a breakout column (`[:field …]` or
   `[:expression …]`, resolved by `token->var`) or an aggregation by index
   (`[:aggregation N]`)."
  [order-by token->var]
  (when (seq order-by)
    (let [parts (for [[dir tok & _] order-by
                      :let [v (cond
                                (and (vector? tok) (= :aggregation (first tok)))
                                (str "ag_" (second tok))
                                (and (vector? tok) (#{:field :expression} (first tok)))
                                (token->var tok)
                                :else nil)]
                      :when v]
                  (str (str/upper-case (name dir)) "(?" v ")"))]
      (when (seq parts)
        (str "ORDER BY " (str/join " " parts))))))

(declare compile-stage)

(defn- resolve-expected-var
  "Resolve the SPARQL variable a stage already projects for one Lib expected column,
   using the variable maps the stage compiler has built. Returns nil when the stage
   compiler did not project that column (so the caller must synthesize it).

   Lib's `result-metadata/returned-columns` strips `:lib/join-alias` from columns
   that came from implicit joins and stamps `:fk-field-id` instead. We use
   `fk-fid->alias` (built from the stage's `:joins`) to recover the originating
   alias and look the qualified var up via `pair->target-var`."
  [col field-id->var pair->target-var fk-fid->alias]
  (let [fid     (:id col)
        alias   (:lib/join-alias col)
        fk-fid  (:fk-field-id col)
        expr-nm (or (:lib/expression-name col)
                    (when (= :source/expressions (:lib/source col)) (:name col)))
        recovered-alias (when (and (not alias) fk-fid)
                          (get fk-fid->alias fk-fid))]
    (cond
      expr-nm                   (sanitize-var-name expr-nm)
      (and fid alias)           (get pair->target-var [fid alias])
      (and fid recovered-alias) (get pair->target-var [fid recovered-alias])
      (and fid (id-field? fid)) "subject"
      fid                       (get field-id->var fid))))

(defn- reconcile-base-projection
  "Build the SELECT variable list (plus any extra OPTIONAL triples) for a base stage so
   it projects exactly one variable per Lib `expected-cols` entry, in Lib's order.

   `expected-cols` is the authoritative column list from Metabase's result-metadata
   (`metabase.lib.metadata.result-metadata/returned-columns`) — the same calculation the
   `annotate` middleware uses. Columns the stage compiler already covers reuse their
   variable; columns it missed (e.g. an FK-remap layered on another FK-remap target)
   are synthesized here so the driver's column count can never drift from Lib's.
   A column it cannot resolve at all gets an unbound `?undefined_N` placeholder,
   so its values come back nil.

   Returns `{:vars [...] :triples [...]}`."
  [expected-cols {:keys [field-id->var pair->target-var alias->intermediate-var
                         fk-fid->alias join-path naming joined-field-vars lang-guard]
                  :or   {lang-guard (constantly nil)}}]
  (let [placeholder (atom 0)
        seen        (atom {})]
    (reduce
     (fn [acc col]
       (let [fid      (:id col)
             alias    (or (:lib/join-alias col)
                          (when-let [fk-fid (:fk-field-id col)]
                            (get fk-fid->alias fk-fid)))
             shared   [fid (:lib/join-alias col)]
             in-order (get-in joined-field-vars [shared (get @seen shared 0)])
             existing (resolve-expected-var col field-id->var pair->target-var fk-fid->alias)]
         (cond
           in-order
           (do (swap! seen update shared (fnil inc 0))
               (update acc :vars conj in-order))

           existing
           (update acc :vars conj existing)

           ;; Joined column the compiler missed: bind it off the join's intermediate var,
           ;; behind the join's FK path (see compile-base-stage).
           (and fid alias (get alias->intermediate-var alias))
           (let [inter (get alias->intermediate-var alias)]
             (if (id-field? fid)
               (update acc :vars conj inter)
               (let [nm   (:name (field-id->metadata fid))
                     v    (joined-var-name alias (or nm (str "f_" fid)))
                     prop (uri/absolute-uri nm naming)]
                 (-> acc
                     (update :vars conj v)
                     (update :triples conj
                             (emit-optional-group
                              (cond-> (conj (vec (join-path alias)) (triple-pattern inter prop v))
                                (lang-guard fid v) (conj (lang-guard fid v)))))))))

           ;; Direct column the compiler missed: bind it off ?subject.
           (and fid (not alias) (not (id-field? fid)) (:name (field-id->metadata fid)))
           (let [nm   (:name (field-id->metadata fid))
                 v    (sanitize-var-name nm)
                 prop (uri/absolute-uri nm naming)]
             (-> acc
                 (update :vars conj v)
                 (update :triples conj
                         (ensure-triple-for-field prop v (lang-guard fid v)))))

           (and fid (id-field? fid))
           (update acc :vars conj "subject")

           ;; Unresolvable column (e.g. an expression): project an unbound placeholder
           ;; so the column count still matches Lib; the value comes back as nil.
           :else
           (update acc :vars conj (str "undefined_" (swap! placeholder inc))))))
     {:vars [] :triples []}
     expected-cols)))

(defn- compile-base-stage
  "Compile a base MBQL stage (one with `:source-table`) to a SPARQL query.

   Left joins (implicit ones added by `add-implicit-joins`, e.g. for FK-remap
   dimensions, as well as explicit notebook joins) are emitted as OPTIONALs
   that each repeat the join's FK path from `?subject`:

     OPTIONAL { ?subject <fk-prop> ?<alias>_subject . }
     OPTIONAL { ?subject <fk-prop> ?<alias>_subject .
                ?<alias>_subject <target-prop> ?<alias>__<field-name> . }

   Without the path, a row whose FK is missing would leave `?<alias>_subject`
   unbound, and the second OPTIONAL would bind it to every node carrying
   `<target-prop>`. For chained joins (e.g. Item → Provider → Owner) the FK
   field lives on a previously joined table; `alias->source-var` resolves each
   hop's source to that prior join's intermediate var, and the path chains
   the hops.

   Aggregation and breakout-only queries project only breakout columns and
   aggregate expressions, with a `GROUP BY` over the breakouts. Throws when a
   `:source-field` the stage would follow is not a foreign key.

   When `expected-cols` (Lib's authoritative column list) is supplied for a
   non-aggregation stage, the SELECT projection is reconciled against it so the
   driver's column count and order always match what the `annotate` middleware
   expects — see [[reconcile-base-projection]].

   Returns `{:sparql <query string> :vars <SELECT var names, in order>
              :aliases <raw column var → temporal bucket var>}`."
  [inner expected-cols]
  (let [limit         (:limit inner)
        table-id      (:source-table inner)
        class-uri     (table-id->class-uri table-id)
        naming        (database-naming-context)
        fields        (:fields inner)
        order-by      (:order-by inner)
        filter-clause (:filter inner)
        joins         (:joins inner)
        aggregations  (:aggregation inner)
        breakout      (:breakout inner)
        ;; Grouped mode: a breakout without aggregations still groups (distinct
        ;; values, e.g. the query behind a field's filter-value list).
        agg?          (boolean (or (seq aggregations) (seq breakout)))
        expressions   (:expressions inner)
        ;; In aggregation mode raw :fields are not projected; the columns that
        ;; need WHERE triples are the breakout columns and the aggregated columns.
        output-tokens (if agg?
                        (vec (concat breakout (keep aggregation-arg-token aggregations)))
                        (vec (or fields [])))
        ;; A synthetic inner query so collect-field-ids / collect-joined-pairs
        ;; pick up breakout + aggregation-arg fields the same way they do :fields.
        triple-inner  (assoc inner :fields output-tokens)
        _             (log/debugf "[mbql] Start compile: table-id=%s agg?=%s output-fields=%d breakout=%d order-by=%d joins=%d"
                                  table-id agg? (count output-tokens) (count breakout) (count order-by) (count joins))
        ;; Joined-field references: `[field-id alias source-field]` that appear anywhere in the stage.
        joined-pairs   (collect-joined-pairs triple-inner)
        ;; Per-join intermediate var: ?<alias>_subject — binds the joined entity URI.
        alias->intermediate-var (into {}
                                      (for [j joins]
                                        [(:alias j) (sanitize-var-name (str (:alias j) "_subject"))]))
        ;; FK property to reach the joined entity. Implicit joins carry `:fk-field-id`;
        ;; explicit joins only have a `:condition`, so fall back to that.
        alias->fk-prop (into {}
                             (for [j joins
                                   :let [alias (:alias j)
                                         fk-id (or (:fk-field-id j)
                                                   (condition->fk-field-id (:condition j) alias))
                                         nm    (when fk-id (:name (field-id->metadata fk-id)))]
                                   :when nm]
                               [alias (uri/absolute-uri nm naming)]))
        ;; LHS of each join's FK triple. Chained joins (e.g. Item → Provider → Owner)
        ;; carry an FK field that lives on a *previously joined* table; emitting the
        ;; triple off `?subject` would silently produce an unbound chain. Resolution
        ;; (most specific first):
        ;;   1. The FK field token in the condition has `:join-alias "Prev"` set —
        ;;      that's the source join's alias directly. (Explicit chained joins.)
        ;;   2. The FK field's `:table-id` matches a previously joined `:source-table`.
        ;;      (Implicit chained joins, where the FK token has no alias.)
        ;;   3. Fallback: `?subject` (single-hop joins anchored on the source table).
        table-id->alias (into {} (for [j joins :when (:source-table j)]
                                   [(:source-table j) (:alias j)]))
        alias->source-var
        (into {}
              (for [j joins
                    :let [alias        (:alias j)
                          fk-ref       (condition->fk-ref (:condition j) alias)
                          fk-tok-alias (field-token->join-alias fk-ref)
                          fk-id        (or (:fk-field-id j) (field-token->id fk-ref))
                          parent-tid   (some-> fk-id field-id->metadata :table-id)
                          src-alias    (or fk-tok-alias
                                           (when (and parent-tid (not= parent-tid table-id))
                                             (get table-id->alias parent-tid)))
                          src-var      (cond
                                         (and src-alias (not= src-alias alias))
                                         (get alias->intermediate-var src-alias)

                                         (and parent-tid
                                              (not= parent-tid table-id)
                                              (nil? src-alias))
                                         (do (log/warnf
                                              "[mbql] FK chain: join %s references table-id %s but no prior join produces it; defaulting source to ?subject"
                                              alias parent-tid)
                                             "subject")

                                         :else "subject")]]
                [alias src-var]))
        ;; `:fk-field-id` → join alias. Lib's result-metadata strips `:lib/join-alias`
        ;; from implicitly-joinable columns and stamps `:fk-field-id` instead; we use
        ;; this map to recover the originating join from an expected-cols entry.
        fk-fid->alias (into {}
                            (for [j joins
                                  :let [fk-id (or (:fk-field-id j)
                                                  (condition->fk-field-id (:condition j) (:alias j)))]
                                  :when fk-id]
                              [fk-id (:alias j)]))
        ;; FK triple patterns from ?subject down to `alias`'s intermediate var, one
        ;; per hop. nil when a hop cannot be resolved (no FK property).
        inter-var->alias (set/map-invert alias->intermediate-var)
        join-path (fn join-path
                    ([alias] (join-path alias #{}))
                    ([alias seen]
                     (let [src    (get alias->source-var alias "subject")
                           fk     (get alias->fk-prop alias)
                           inter  (get alias->intermediate-var alias)
                           parent (get inter-var->alias src)]
                       (when (and fk inter (not (seen alias)))
                         (let [prefix (when parent (join-path parent (conj seen alias)))]
                           (when (or (nil? parent) prefix)
                             (conj (vec prefix) (triple-pattern src fk inter))))))))
        ;; The display value of a FK column inside an explicit join, e.g.
        ;; `[:field <City.label> {:join-alias "C" :source-field <C.headquarters>}]`,
        ;; sits one hop past the joined entity. It is keyed with its `:source-field`,
        ;; since the join can reach the same field directly or through another FK.
        ;; An implicit join's tokens carry its own FK as `:source-field`: no hop.
        implicit-aliases (set (keep #(when (:fk-field-id %) (:alias %)) joins))
        joined-keys (set (for [[fid alias sf] joined-pairs]
                           (if (and sf (not (implicit-aliases alias))) [fid alias sf] [fid alias])))
        pair->hop (into {}
                        (for [[_ alias sf :as k] joined-keys
                              :when sf
                              :let [nm (:name (field-id->metadata sf))]
                              :when nm]
                          [k {:name nm
                              :prop (uri/absolute-uri nm naming)
                              :var  (joined-var-name alias (str nm "_subject"))}]))
        ;; Per joined-pair: the SPARQL var that carries the value. The joined entity's
        ;; own subject column IS the intermediate var (no extra triple needed); every
        ;; other joined column gets a unique `<alias>__[<hop>__]<field-name>` var.
        pair->target-var (into {}
                               (for [[fid alias :as k] joined-keys]
                                 [k
                                  (if (id-field? fid)
                                    (or (:var (pair->hop k)) (get alias->intermediate-var alias))
                                    (joined-var-name alias
                                                     (str (some-> (pair->hop k) :name (str "__"))
                                                          (or (:name (field-id->metadata fid))
                                                              (str "f_" fid)))))]))
        ;; A `:source-field` outside an implicit join must be a FK: Metabase adds no
        ;; join for one that is not (auto sync has none), and inside an explicit join
        ;; the display value hops through it.
        _ (when-let [sf (->> (tree-seq coll? seq (select-keys triple-inner [:fields :order-by :filter :expressions]))
                             (some #(when (and (vector? %) (= :field (first %)))
                                      (let [{sf :source-field alias :join-alias} (field-token->opts %)]
                                        (when (and sf (not (implicit-aliases alias))
                                                   (or (nil? alias)
                                                       (not= :type/FK (:semantic-type (field-id->metadata sf)))))
                                          sf)))))]
            (throw (ex-info (format "The SPARQL driver cannot follow %s: it is not a foreign key."
                                    (let [meta (field-id->metadata sf)] (or (:display-name meta) (:name meta) sf)))
                            {:type driver-api/qp.error-type.unsupported-feature})))
        ;; Field-ids read off the row itself (a field can also be reached through a
        ;; join, e.g. a self-referencing FK's display value).
        field-ids     (collect-field-ids triple-inner)
        field-id->prop (into {}
                             (for [fid field-ids
                                   :let [nm (:name (field-id->metadata fid))]
                                   :when (and nm (not (id-field? fid)))]
                               [fid (uri/absolute-uri nm naming)]))
        field-id->var  (build-var-aliases field-ids)
        token->var     (fn [tok] (var-for-token tok field-id->var pair->target-var))
        ;; With a Default Language, `rdf:langString` columns only take values in
        ;; that language (or untagged), filtered inside their OPTIONAL.
        lang           (let [l (database-default-language)] (when-not (str/blank? l) l))
        lang-guard     (fn [fid var] (when (and lang var (lang-string-field? fid)) (lang-filter var lang)))
        triples-for-fields (->> output-tokens
                                (keep (fn [tok]
                                        (let [fid   (field-token->id tok)
                                              alias (field-token->join-alias tok)]
                                          (when (and fid
                                                     (not alias)
                                                     (not (id-field? fid))
                                                     (get field-id->prop fid))
                                            (ensure-triple-for-field (get field-id->prop fid)
                                                                     (get field-id->var fid)
                                                                     (lang-guard fid (get field-id->var fid))))))))
        ;; Field tokens appearing in order-by/filter but not in fields (still need their triple).
        extra-direct-fids (letfn [(collect-field-tokens [x]
                                    (cond
                                      (and (vector? x) (= :field (first x))) [x]
                                      (sequential? x) (mapcat collect-field-tokens x)
                                      (map? x) (mapcat collect-field-tokens (vals x))
                                      :else []))]
                            (->> (concat (mapcat (fn [[_dir fld & _]] [fld]) (or order-by []))
                                         (collect-field-tokens filter-clause)
                                         (collect-expression-tokens expressions))
                                 (remove field-token->join-alias)
                                 (keep field-token->id)
                                 set
                                 (remove (set (keep field-token->id (remove field-token->join-alias output-tokens))))
                                 (remove id-field?)))
        triples-for-extras (for [fid extra-direct-fids
                                 :let [prop (get field-id->prop fid)
                                       var  (get field-id->var fid)]
                                 :when (and prop var)]
                             (ensure-triple-for-field prop var (lang-guard fid var)))
        ;; OPTIONAL triples introduced by left-joins.
        join-fk-triples (for [j joins
                              :let [path (join-path (:alias j))]
                              :when path]
                          (emit-optional-group path))
        ;; One triple per joined column. A subject column needs no triple: it IS the
        ;; intermediate (or hop) var, already bound by the FK triple.
        join-target-triples (for [[fid alias :as k] joined-keys
                                  :let [hop (pair->hop k)
                                        id? (id-field? fid)]
                                  :when (or hop (not id?))
                                  :let [nm (:name (field-id->metadata fid))
                                        prop (uri/absolute-uri nm naming)
                                        target-var (get pair->target-var k)
                                        inter-var (get alias->intermediate-var alias)
                                        path (join-path alias)]
                                  :when (and prop target-var path)]
                              (emit-optional-group
                               (concat path
                                       (when hop [(triple-pattern inter-var (:prop hop) (:var hop))])
                                       (when-not id?
                                         (cond-> [(triple-pattern (:var hop inter-var) prop target-var)]
                                           (lang-guard fid target-var) (conj (lang-guard fid target-var)))))))
        _ (log/debugf "[mbql] Triples: fields=%d extras=%d join-fk=%d join-targets=%d"
                      (count triples-for-fields) (count triples-for-extras)
                      (count join-fk-triples) (count join-target-triples))
        filters (when filter-clause
                  (or (compile-basic-filter filter-clause field-id->var pair->target-var)
                      []))
        _ (log/debugf "[mbql] Filters count: %d" (count filters))
        ;; --- SELECT / GROUP BY / ORDER BY ------------------------------------
        agg-projections (when agg?
                          (map-indexed (fn [i a] (aggregation->projection a i token->var))
                                       aggregations))
        bucketed        (bucket-breakout breakout token->var)
        breakout-vars   (when agg? (:vars bucketed))
        ;; Custom-column BINDs. Emitted after the triples that bind the variables
        ;; they reference (direct fields, extras, joined targets) so the values are
        ;; available; placed before filters/GROUP BY/ORDER BY which may use them.
        expr-bind-lines (compile-expressions expressions field-id->var pair->target-var
                                             (set (concat ["subject"]
                                                          (vals field-id->var)
                                                          (vals pair->target-var)
                                                          (vals alias->intermediate-var)
                                                          (keep :var (vals pair->hop))
                                                          (vals (:aliases bucketed))
                                                          (map :var agg-projections))))
        _ (log/debugf "[mbql] Expression BINDs: %d" (count expr-bind-lines))
        ;; Non-aggregation SELECT var list: ?subject + direct fields + joined target vars.
        direct-select-vars (when-not agg?
                             (->> fields
                                  (remove field-token->join-alias)
                                  (keep field-token->id)
                                  (remove id-field?)
                                  (map field-id->var)
                                  (remove nil?)
                                  distinct
                                  vec))
        joined-select-vars (when-not agg?
                             (->> fields
                                  (keep #(when (field-token->join-alias %) (token->var %)))
                                  distinct
                                  vec))
        ;; Expression columns explicitly projected via :fields (fallback path only;
        ;; the reconcile path resolves them positionally against Lib's expected-cols).
        expr-select-vars (when-not agg?
                           (->> fields
                                (filter expression-token?)
                                (map token->var)
                                distinct
                                vec))
        ;; Lib describes a joined column as `{:id f :lib/join-alias a}`, without the
        ;; `:source-field` of a FK display value inside an explicit join, so a field
        ;; reached directly and through a FK look alike. They come in `:fields` order,
        ;; so joined columns take their vars in that order.
        joined-field-vars (->> fields
                               (filter field-token->join-alias)
                               (group-by (juxt field-token->id field-token->join-alias))
                               (into {} (map (fn [[k toks]] [k (mapv token->var toks)]))))
        ;; When Lib's expected columns are known, reconcile the SELECT against them so
        ;; the driver's column count/order can never drift from the `annotate` middleware.
        reconciled  (when (and expected-cols (not agg?))
                      (reconcile-base-projection
                       expected-cols
                       {:field-id->var           field-id->var
                        :pair->target-var        pair->target-var
                        :alias->intermediate-var alias->intermediate-var
                        :fk-fid->alias           fk-fid->alias
                        :join-path               join-path
                        :naming                  naming
                        :joined-field-vars       joined-field-vars
                        :lang-guard              lang-guard}))
        result-vars (cond
                      agg?       (vec (concat breakout-vars (keep :var agg-projections)))
                      reconciled (vec (:vars reconciled))
                      :else      (vec (concat ["subject"] direct-select-vars joined-select-vars expr-select-vars)))
        select-part (if agg?
                      (str "SELECT "
                           (str/join " " (concat (map #(str "?" %) breakout-vars)
                                                 (map :select agg-projections))))
                      (str "SELECT " (str/join " " (map #(str "?" %) result-vars))))
        group-by-clause (when (and agg? (seq breakout-vars))
                          (str "GROUP BY " (str/join " " (map #(str "?" %) breakout-vars))))
        order-clause (if agg?
                       (compile-agg-order-by order-by (:token->var bucketed))
                       (compile-order-by order-by field-id->var pair->target-var))
        where-body  (->> (concat [(format "  ?subject a %s ." (uri/iri-ref class-uri))]
                                 triples-for-fields
                                 triples-for-extras
                                 join-fk-triples
                                 join-target-triples
                                 (or (:triples reconciled) [])
                                 expr-bind-lines
                                 (:binds bucketed)
                                 filters)
                         (str/join "\n"))
        where-part  (str "WHERE {\n" where-body "\n}")
        limit-part  (when (number? limit) (str "LIMIT " limit))
        query       (str (str/trim select-part) "\n"
                         where-part "\n"
                         (when group-by-clause (str group-by-clause "\n"))
                         (when order-clause (str order-clause "\n"))
                         (when limit-part (str limit-part)))]
    (log/debugf "[sparql.mbql] Compiled base stage: %s" query)
    {:sparql  query
     :vars    result-vars
     :aliases (:aliases bucketed)}))

(defn- inner-var-for-ref
  "Determine the SPARQL variable a base/inner stage projects for `fk-ref` — a
   `[:field id-or-name …]` token referenced by an outer join condition. The
   inner stage names variables by the sanitized field name, so this stays
   consistent whether the outer condition kept the numeric id or degraded to a
   source-query column name string."
  [fk-ref]
  (let [id (field-token->id fk-ref)]
    (cond
      (and (integer? id) (id-field? id)) "subject"
      (integer? id) (sanitize-var-name (:name (field-id->metadata id)))
      (string? id)  (sanitize-var-name id)
      :else         nil)))

(def ^:private prologue-re
  "Match the prologue at the start of a SPARQL query: whitespace, `#` comments,
   `PREFIX` and `BASE` declarations, and Virtuoso's `DEFINE` pragmas."
  #"^(?:\s+|#[^\n\r]*|(?i:PREFIX)\s+[^\s:]*:\s*<[^>]*>|(?i:BASE)\s*<[^>]*>|(?i:DEFINE)\s+\S+\s+(?:\"[^\"]*\"|\S+))*")

(def ^:private dataset-clause-re
  "Match a `FROM <g>` or `FROM NAMED <g>` dataset clause."
  #"(?i)\bFROM\s+(?:NAMED\s+)?<[^>]*>")

(defn- compile-native-stage
  "Compile a saved native question used as the source of an MBQL stage: its
   SPARQL, with `{{tag}}`s already substituted, becomes the sub-`SELECT`.
   `source-metadata` (the outer stage's view of the native columns) names its
   variables, which also covers `SELECT *`. A sub-`SELECT` can hold neither a
   prologue nor a dataset clause, so the `PREFIX`, `BASE` and `DEFINE` lines
   come back apart as `:prologue`, and the `FROM` clauses before the first `{`
   as `:dataset`, for [[mbql->native]] to put back around the whole query.
   Throws when the source is not a `SELECT`, or when Metabase has not recorded
   its columns yet.

   Returns `{:sparql … :vars … :prologue … :dataset …}` (see
   [[compile-base-stage]] for the first two)."
  [native source-metadata]
  (let [native   (str/replace native #"^\uFEFF" "")
        prologue (re-find prologue-re native)
        body     (subs native (count prologue))
        head-end (or (str/index-of body "{") (count body))
        head     (subs body 0 head-end)]
    (when-not (re-find #"(?i)^SELECT\b" body)
      (throw (ex-info "A question built on a saved native SPARQL question needs a SELECT query as its source."
                      {:type driver-api/qp.error-type.unsupported-feature})))
    (when (empty? source-metadata)
      (throw (ex-info "Metabase has not recorded the columns of this saved native SPARQL question yet. Run it once, then try again."
                      {:type driver-api/qp.error-type.invalid-query})))
    {:sparql   (str (str/replace head dataset-clause-re "") (subs body head-end))
     :vars     (mapv :name source-metadata)
     :prologue (str/trim prologue)
     :dataset  (str/join "\n" (re-seq dataset-clause-re head))}))

(defn- compile-derived-stage
  "Compile a derived MBQL stage (one with `:source-query`). These arise e.g.
   when a saved card / model is used as a source, when an FK display-value
   remap is layered on top of an aggregation, or from user multi-stage queries
   (such as drilling on an aggregation value).

   The inner stage is compiled as a SPARQL sub-`SELECT`; the derived stage's
   remap joins are emitted as OPTIONALs after it. The outer stage's own
   `:aggregation`, `:breakout`, `:expressions`, `:filter`, `:order-by`, and
   explicit `:fields` projection are honored, resolved against the variables
   the sub-`SELECT` already projects (no triple patterns are needed at this
   level).

   When `expected-cols` (Lib's authoritative column list) is supplied for a
   non-aggregation stage, the SELECT projection is reconciled against it so the
   driver's column count and order always match what the `annotate` middleware
   expects.

   Returns `{:sparql … :vars … :aliases …}` (see [[compile-base-stage]]), plus
   the `:prologue` and `:dataset` of a native source ([[compile-native-stage]])."
  [stage expected-cols]
  (let [naming        (database-naming-context)
        source        (:source-query stage)
        native?       (string? (:native source))
        inner         (if native?
                        (compile-native-stage (:native source) (:source-metadata stage))
                        (compile-stage source))
        joins         (:joins stage)
        alias->join   (into {} (for [j joins] [(:alias j) j]))
        remap-entries (for [tok   (:fields stage)
                            :let  [alias (field-token->join-alias tok)]
                            :when alias
                            :let  [tid    (field-token->id tok)
                                   join   (get alias->join alias)
                                   fk-var (some-> join :condition condition->fk-ref inner-var-for-ref)
                                   nm     (when (integer? tid) (:name (field-id->metadata tid)))
                                   prop   (when nm (uri/absolute-uri nm naming))
                                   rvar   (joined-var-name alias (or nm (str "f_" tid)))]
                            :when (and fk-var prop)]
                        {:optional (emit-remap-optional fk-var prop rvar)
                         :var      rvar
                         :tid      tid
                         :alias    alias})
        ;; Columns visible to the outer stage: inner sub-SELECT vars + remapped vars.
        passthrough-vars (vec (concat (:vars inner) (map :var remap-entries)))
        ;; The sub-SELECT already projects these by name, so resolving an outer
        ;; field token (a string-named source-query column) is just sanitizing it;
        ;; a native source's names are its own SPARQL variables, kept as they are
        ;; (`?idade_média` is a valid variable that sanitizing would rename).
        ;; Aggregation results are the exception: the inner stage names them `ag_N`,
        ;; but a later stage references them by Lib's name (`count`, `sum`, …), so we
        ;; add those aliases (drilling on an aggregation value relies on this).
        field-id->var    (merge (into {} (for [v passthrough-vars] [v (if native? v (sanitize-var-name v))]))
                                (aggregation-name->var (:aggregation source))
                                ;; a temporal bucket keeps its raw column name in Lib
                                (:aliases inner))
        pair->target-var (into {} (for [{:keys [tid alias var]} remap-entries]
                                    [[tid alias] var]))
        token->var       (fn [tok] (var-for-token tok field-id->var pair->target-var))
        aggregations  (:aggregation stage)
        breakout      (:breakout stage)
        ;; Grouped mode: a breakout without aggregations still groups (distinct
        ;; values, e.g. the query behind a field's filter-value list).
        agg?          (boolean (or (seq aggregations) (seq breakout)))
        filter-clause (:filter stage)
        order-by      (:order-by stage)
        limit         (:limit stage)
        agg-projections (when agg?
                          (vec (map-indexed (fn [i a] (aggregation->projection a i token->var true))
                                            aggregations)))
        bucketed        (bucket-breakout breakout token->var)
        breakout-vars   (when agg? (:vars bucketed))
        ;; Non-agg explicit projection: resolve every :fields token; fall back to
        ;; passthrough if any token cannot be resolved.
        projected-vars  (when (and (not agg?) (seq (:fields stage)))
                          (let [vs (map token->var (:fields stage))]
                            (when (every? some? vs)
                              (vec (distinct vs)))))
        ;; When Lib's expected columns are known, reconcile the SELECT against them.
        ;; Remap columns resolve via `pair->target-var` (or an extra OPTIONAL); the rest
        ;; match inner vars by desired alias or Lib name (`count` → `ag_0`) before
        ;; position, since an FK remap reorders the sub-SELECT.
        reconciled    (when (and expected-cols (not agg?))
                        (let [inner-vars  (atom (:vars inner))
                              placeholder (atom 0)]
                          (reduce
                           (fn [acc col]
                             (let [tid   (:id col)
                                   alias (:lib/join-alias col)
                                   join  (when alias (get alias->join alias))
                                   fk-var (some-> join :condition condition->fk-ref inner-var-for-ref)
                                   nm    (when (integer? tid) (:name (field-id->metadata tid)))
                                   expr-nm (or (:lib/expression-name col)
                                               (when (= :source/expressions (:lib/source col)) (:name col)))]
                               (cond
                                 expr-nm
                                 (update acc :vars conj (sanitize-var-name expr-nm))

                                 (and alias (get pair->target-var [tid alias]))
                                 (update acc :vars conj (get pair->target-var [tid alias]))

                                 (and alias join fk-var nm)
                                 (let [rvar (joined-var-name alias nm)
                                       prop (uri/absolute-uri nm naming)]
                                   (-> acc
                                       (update :vars conj rvar)
                                       (update :optionals conj
                                               (emit-remap-optional fk-var prop rvar))))

                                 (seq @inner-vars)
                                 (let [unused (set @inner-vars)
                                       v      (or (some unused
                                                        [(some-> (:lib/desired-column-alias col) sanitize-var-name)
                                                         (when-not (:fk-field-id col)
                                                           (get field-id->var (:name col)))])
                                                  (first @inner-vars))]
                                   (swap! inner-vars #(remove #{v} %))
                                   (update acc :vars conj v))

                                 :else
                                 (update acc :vars conj (str "undefined_" (swap! placeholder inc))))))
                           {:vars [] :optionals []}
                           expected-cols)))
        ;; Lib-name → SPARQL-var, so an outer filter/order-by referencing an inner
        ;; column by Lib's name resolves even when the driver's invented var name
        ;; differs (joined breakout columns become `?<alias>__<field>`, aggregations
        ;; become `?ag_N`). `reconciled` produces exactly one var per `expected-col`
        ;; in order, so they zip positionally; both `:name` and any
        ;; `:lib/desired-column-alias` are keyed in case the ref uses either.
        expected-name->var (when reconciled
                             (into {}
                                   (mapcat (fn [col v]
                                             (keep (fn [k] (when k [k v]))
                                                   [(:name col) (:lib/desired-column-alias col)]))
                                           expected-cols (:vars reconciled))))
        ;; The map used to resolve the outer stage's own filter/order-by clauses.
        outer-field-id->var (merge field-id->var expected-name->var)
        ;; Custom columns defined on the derived stage, resolved against the inner
        ;; sub-SELECT's columns (by sanitized name) and any remap vars.
        expressions   (:expressions stage)
        expr-bind-lines (compile-expressions expressions outer-field-id->var pair->target-var
                                             (set (concat passthrough-vars
                                                          (vals (:aliases bucketed))
                                                          (map :var agg-projections))))
        result-vars   (cond
                        agg?           (vec (concat breakout-vars (keep :var agg-projections)))
                        reconciled     (vec (:vars reconciled))
                        projected-vars projected-vars
                        :else          passthrough-vars)
        select-part   (if agg?
                        (str "SELECT "
                             (str/join " " (concat (map #(str "?" %) breakout-vars)
                                                   (map :select agg-projections))))
                        (str "SELECT " (str/join " " (map #(str "?" %) result-vars))))
        group-by-clause (when (and agg? (seq breakout-vars))
                          (str "GROUP BY " (str/join " " (map #(str "?" %) breakout-vars))))
        order-clause  (if agg?
                        (compile-agg-order-by order-by (:token->var bucketed))
                        (compile-order-by order-by outer-field-id->var pair->target-var))
        filters       (when filter-clause
                        (or (compile-basic-filter filter-clause outer-field-id->var pair->target-var) []))
        query         (str (str/trim select-part) "\n"
                           "WHERE {\n"
                           "  {\n" (:sparql inner) "\n  }\n"
                           (when (seq remap-entries)
                             (str (str/join "\n" (map :optional remap-entries)) "\n"))
                           (when (seq (:optionals reconciled))
                             (str (str/join "\n" (:optionals reconciled)) "\n"))
                           (when (seq expr-bind-lines)
                             (str (str/join "\n" expr-bind-lines) "\n"))
                           (when (seq (:binds bucketed))
                             (str (str/join "\n" (:binds bucketed)) "\n"))
                           (when (seq filters)
                             (str (str/join "\n" filters) "\n"))
                           "}\n"
                           (when group-by-clause (str group-by-clause "\n"))
                           (when order-clause (str order-clause "\n"))
                           (when (number? limit) (str "LIMIT " limit)))]
    (log/debugf "[sparql.mbql] Compiled derived stage: %s" query)
    {:sparql   query
     :vars     result-vars
     :aliases  (:aliases bucketed)
     :prologue (:prologue inner)
     :dataset  (:dataset inner)}))

(defn- compile-stage
  "Compile one MBQL stage, recursing through `:source-query` wrappers.

   `expected-cols` (Lib's authoritative column list) is only supplied for the
   outermost stage — the one whose SELECT becomes the query's result columns.
   Inner sub-`SELECT`s recurse with `nil`."
  ([stage] (compile-stage stage nil))
  ([stage expected-cols]
   (if (:source-query stage)
     (compile-derived-stage stage expected-cols)
     (compile-base-stage stage expected-cols))))

(defn- expected-result-columns
  "Return Lib's authoritative result columns for `outer-query` (pMBQL) — the same calculation
   the `annotate` middleware uses to decide how many columns the query should return.
   Returns nil if Lib cannot compute them, in which case the compiler falls back to
   deriving the projection from the query's own `:fields`."
  [outer-query]
  (try
    (not-empty (result-metadata/returned-columns outer-query []))
    (catch Exception e
      (log/warnf "[sparql.mbql] Could not compute expected columns from Lib: %s"
                 (.getMessage e))
      nil)))

(defn mbql->native
  "Compile a Metabase query into a native SPARQL map `{:query <string> :mbql? true}`.

   Metabase passes pMBQL (Lib / MBQL 5). Before converting to legacy MBQL we ask Lib
   for the query's authoritative result columns ([[expected-result-columns]]) and
   reconcile the outermost SELECT against them, so the driver's column count and order
   can never drift from what the `annotate` middleware expects.

   Multi-stage queries are compiled stage by stage (inner stage → sub-`SELECT`).
   The prologue and dataset clauses of a saved native source go back at the top
   and before the outer `WHERE` (see [[compile-native-stage]]).
   See [[compile-base-stage]] and [[compile-derived-stage]]."
  [_driver outer-query]
  (let [expected-cols    (expected-result-columns outer-query)
        legacy-query     (driver-api/->legacy-MBQL outer-query)
        {:keys [sparql vars prologue dataset]} (compile-stage (:query legacy-query) expected-cols)
        sparql           (cond->> sparql
                           (seq dataset)  (#(str/replace-first % "\nWHERE {" (str "\n" dataset "\nWHERE {")))
                           (seq prologue) (str prologue "\n"))]
    (log/debugf "[sparql.mbql->native] Compiled query (%d projected columns): %s"
                (count vars) sparql)
    {:query sparql
     :mbql? true}))


