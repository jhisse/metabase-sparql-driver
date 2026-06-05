(ns metabase.driver.sparql.mbql
  "Simple MBQL → SPARQL transpilation. Supports projection, filters, ordering,
   implicit joins, and aggregations."
  (:require
   [clojure.string :as str]
   [clojure.set :as set]
   [metabase.driver-api.core :as driver-api]
   [metabase.driver.sparql.uri :as uri]
   [metabase.lib.metadata.result-metadata :as result-metadata]
   [metabase.util.log :as log]))

(defn- sanitize-var-name
  "Return a SPARQL-safe variable name."
  [s]
  (let [base (-> (str s)
                 (str/replace #"[^A-Za-z0-9_]" "_")
                 (str/replace #"^([0-9])" "_$1"))]
    (if (str/blank? base) "v" base)))

(defn- field-token->id
  "Extract Field ID from a [:field id opts] token."
  [field-token]
  (when (and (vector? field-token)
             (= :field (first field-token)))
    (second field-token)))

(defn- field-token->opts
  "Extract the options map from a [:field id opts] token, if any."
  [field-token]
  (when (and (vector? field-token) (= :field (first field-token)))
    (let [opts (nth field-token 2 nil)]
      (when (map? opts) opts))))

(defn- field-token->join-alias
  "Return the `:join-alias` on a field token (the marker that the field is reached via an implicit join)."
  [field-token]
  (:join-alias (field-token->opts field-token)))

(declare collect-expression-tokens)

(defn- expression-token?
  "True for an `[:expression \"name\" opts?]` reference (a custom column)."
  [token]
  (and (vector? token) (= :expression (first token))))

(defn- expression-token->name
  "The expression name referenced by an `[:expression \"name\"]` token."
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
  "Return true if the field-id represents the synthetic subject column.

   The only reliable signal is the reserved name `subject` that
   `build-pk-field` hardcodes. We must NOT use `:semantic-type :type/PK`:
   Metabase's name-based classifier auto-stamps `:type/PK` on *any* field
   literally named `id`, and after Default-Graph URI shortening a real RDF
   property `<base>/id` becomes a column named `id`. Treating it as the
   subject would silently drop it from the SELECT."
  [field-id]
  (= "subject" (:name (field-id->metadata field-id))))

(defn- database-default-graph
  "Read the Default Graph URI from the connection details of the current database."
  []
  (some-> (driver-api/database (driver-api/metadata-provider))
          :details
          :default-graph))

(defn- database-default-language
  "Read the Default Language (BCP-47 tag) from connection details. May be blank."
  []
  (some-> (driver-api/database (driver-api/metadata-provider))
          :details
          :default-language))

(defn- lang-string-field?
  "True when the field's `:database-type` indicates an `rdf:langString` property
   (set during SHACL sync). Other sync strategies always emit `\"string\"`, so
   this is effectively a SHACL-only signal."
  [field-id]
  (= "langString" (:database-type (field-id->metadata field-id))))

(defn- geometry-field?
  "True when the field's `:database-type` marks it as an RDF geometry/WKT column
   (set during sync — see `metabase.driver.sparql.conversion`). Equality on such
   a field must compare the lexical form via `STR()`: the stored term is a typed
   geometry literal (e.g. `\"POINT(...)\"^^virtrdf:Geometry`) that never compares
   equal to a plain string literal, so a direct `=` silently returns no rows."
  [field-id]
  (= "geometry" (:database-type (field-id->metadata field-id))))

(defn- lang-filter-line
  "Render the LANG filter clause for one variable. Guards against unbound
   variables (left joins) and accepts untagged literals alongside the target
   language."
  [var-name lang]
  (format "  FILTER(!BOUND(?%s) || LANG(?%s) = \"%s\" || LANG(?%s) = \"\")"
          var-name var-name lang var-name))

(defn- table-id->class-uri
  "Resolve RDF class URI (table name) from :source-table."
  [table-id]
  (let [nm  (some-> (driver-api/table (driver-api/metadata-provider) table-id)
                    :name)
        uri (uri/absolute-uri nm (database-default-graph))]
    (log/debugf "[mbql] Resolved class URI for table-id %s: %s" table-id uri)
    uri))

(defn- collect-field-ids
  "Collect referenced field IDs from fields/order-by/filter/expressions."
  [{:keys [fields order-by expressions] filter-clause :filter}]
  (let [ids-from-fields (set (keep field-token->id fields))
        ids-from-order  (set (keep (fn [[_dir fld & _]] (field-token->id fld)) order-by))
        ids-from-filter (letfn [(collect-field-tokens [x]
                                  (cond
                                    (and (vector? x) (= :field (first x))) [x]
                                    (sequential? x) (mapcat collect-field-tokens x)
                                    (map? x) (mapcat collect-field-tokens (vals x))
                                    :else []))]
                          (set (keep field-token->id (collect-field-tokens filter-clause))))
        ids-from-expr   (set (keep field-token->id (collect-expression-tokens expressions)))
        all-ids         (vec (set/union ids-from-fields ids-from-order ids-from-filter ids-from-expr))]
    (log/debugf "[mbql] Collected field IDs: fields=%d order=%d filter=%d total=%d"
                (count ids-from-fields) (count ids-from-order) (count ids-from-filter) (count all-ids))
    all-ids))

(defn- var-for-token
  "Resolve the SPARQL variable name for a `[:field id opts]` token.

   - If the token carries `:join-alias`, look up the joined target var via
     `pair->target-var` keyed by `[field-id alias]`.
   - Otherwise the subject (PK) field always maps to `?subject`.
   - Otherwise look up the regular alias from `field-id->var`."
  [field-token field-id->var pair->target-var]
  (if (expression-token? field-token)
    (sanitize-var-name (expression-token->name field-token))
    (let [fid   (field-token->id field-token)
          alias (field-token->join-alias field-token)]
      (cond
        (and fid alias) (get pair->target-var [fid alias])
        (and fid (id-field? fid)) "subject"
        fid (get field-id->var fid)))))

(defn- condition->fk-ref
  "Extract the FK-source field token from a join `:condition`. The condition is
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
  "Field-id of the FK-source side of a join `:condition` (see [[condition->fk-ref]])."
  ([condition] (field-token->id (condition->fk-ref condition)))
  ([condition this-alias] (field-token->id (condition->fk-ref condition this-alias))))

(defn- collect-joined-pairs
  "Walk a legacy-MBQL stage and return the set of `[field-id alias]` pairs for every
   `[:field id {:join-alias \"...\"}]` token that appears in `:fields`, `:order-by`,
   or `:filter`."
  [{:keys [fields order-by expressions] filter-clause :filter}]
  (letfn [(walk [x]
            (cond
              (and (vector? x) (= :field (first x)))
              (when-let [a (field-token->join-alias x)]
                [[(field-token->id x) a]])
              (sequential? x) (mapcat walk x)
              (map? x) (mapcat walk (vals x))
              :else []))]
    (set (concat (mapcat walk (or fields []))
                 (mapcat walk (mapcat (fn [[_dir fld & _]] [fld]) (or order-by [])))
                 (mapcat walk (collect-expression-tokens expressions))
                 (walk filter-clause)))))

(defn- literal->sparql
  "Convert a value to a SPARQL literal."
  [v]
  (cond
    (string? v) (str "\"" (str/replace v "\"" "\\\"") "\"")
    (number? v) (str v)
    (boolean? v) (if v "true" "false")
    (nil? v) ""
    :else (str "\"" (str/replace (str v) "\"" "\\\"") "\"")))

(defn- url-shaped?
  "True when `v` is a string carrying a URI scheme (`http:`, `https:`, `urn:`, …).
   Uses the same scheme regex as [[uri/absolute-uri]]."
  [v]
  (boolean (and (string? v) (re-find #"^[A-Za-z][A-Za-z0-9+.-]*:" v))))

(defn- iri-valued-field?
  "True when the field's metadata marks it as carrying an RDF IRI value — a
   foreign key (`:type/FK`) or a URL column (`:type/URL`). These are bound to IRI
   nodes in SPARQL, so an equality filter must compare against `<iri>`, not a
   quoted string literal."
  [field-id]
  (let [meta (field-id->metadata field-id)]
    (or (contains? #{:type/FK :type/URL} (:semantic-type meta))
        (= :type/URL (:base-type meta)))))

(defn- value->term
  "Render a filter RHS value as a SPARQL term for an equality comparison.
   When the field is IRI-valued and the value is a URL-shaped string, emit an
   IRI `<…>` so it matches the bound node; otherwise fall back to a literal."
  [field-id v]
  (if (and (iri-valued-field? field-id) (string? v) (url-shaped? v))
    (str "<" v ">")
    (literal->sparql v)))

(defn- compile-filter-expr
  "Compile a filter clause to a SPARQL boolean expression string."
  [filter-clause field-id->var pair->target-var]
  (when (sequential? filter-clause)
    (let [[op lhs rhs maybe-opts & more] filter-clause]
      (case op
        :and (let [parts (->> (concat [lhs rhs] more)
                              (keep #(compile-filter-expr % field-id->var pair->target-var)))]
               (when (seq parts)
                 (str "(" (str/join " && " parts) ")")))
        :or  (let [parts (->> (concat [lhs rhs] more)
                              (keep #(compile-filter-expr % field-id->var pair->target-var)))]
               (when (seq parts)
                 (str "(" (str/join " || " parts) ")")))
        :not (when-let [inner (compile-filter-expr lhs field-id->var pair->target-var)]
               (str "(!" inner ")"))
        ;; [:inside lat lon lat-max(N) lon-min(W) lat-min(S) lon-max(E)] — the
        ;; map "draw a box" filter. lat/lon are (typically synthesized) coordinate
        ;; columns; compile to a bounding-box range over both.
        :inside (let [[_ lat-tok lon-tok lat-max lon-min lat-min lon-max] filter-clause
                      latv (var-for-token lat-tok field-id->var pair->target-var)
                      lonv (var-for-token lon-tok field-id->var pair->target-var)
                      uv   (fn [x] (if (and (vector? x) (= :value (first x))) (second x) x))]
                  (when (and latv lonv)
                    (format "((?%s <= %s) && (?%s >= %s) && (?%s >= %s) && (?%s <= %s))"
                            latv (literal->sparql (uv lat-max))
                            latv (literal->sparql (uv lat-min))
                            lonv (literal->sparql (uv lon-min))
                            lonv (literal->sparql (uv lon-max)))))
        (let [fid (field-token->id lhs)
              var (var-for-token lhs field-id->var pair->target-var)
              opts (when (map? maybe-opts) maybe-opts)
              insensitive? (false? (:case-sensitive opts))
              v (let [x rhs]
                  (if (and (vector? x) (= :value (first x)))
                    (second x)
                    x))]
          (when var
            (case op
              := (cond
                   (nil? v)              (format "(!BOUND(?%s))" var)
                   (geometry-field? fid) (format "(STR(?%s) = %s)" var (literal->sparql v))
                   :else                 (format "(?%s = %s)" var (value->term fid v)))
              :!= (cond
                    (nil? v)              (format "(BOUND(?%s))" var)
                    (geometry-field? fid) (format "(STR(?%s) != %s)" var (literal->sparql v))
                    :else                 (format "(?%s != %s)" var (value->term fid v)))
              :> (and (some? v) (format "(?%s > %s)" var (literal->sparql v)))
              :>= (and (some? v) (format "(?%s >= %s)" var (literal->sparql v)))
              :< (and (some? v) (format "(?%s < %s)" var (literal->sparql v)))
              :<= (and (some? v) (format "(?%s <= %s)" var (literal->sparql v)))
              ;; [:between field min max] — min is rhs (`v`), max is the next arg.
              :between (let [hi (let [x maybe-opts]
                                  (if (and (vector? x) (= :value (first x))) (second x) x))]
                         (when (and (some? v) (some? hi))
                           (format "(?%s >= %s && ?%s <= %s)"
                                   var (literal->sparql v) var (literal->sparql hi))))
              :starts-with (let [needle (literal->sparql v)
                                 expr (if insensitive?
                                        (format "STRSTARTS(LCASE(STR(?%s)), LCASE(%s))" var needle)
                                        (format "STRSTARTS(STR(?%s), %s)" var needle))]
                             (str "(" expr ")"))
              :ends-with (let [needle (literal->sparql v)
                               expr (if insensitive?
                                      (format "STRENDS(LCASE(STR(?%s)), LCASE(%s))" var needle)
                                      (format "STRENDS(STR(?%s), %s)" var needle))]
                           (str "(" expr ")"))
              :contains (let [needle (literal->sparql v)
                              expr (if insensitive?
                                     (format "CONTAINS(LCASE(STR(?%s)), LCASE(%s))" var needle)
                                     (format "CONTAINS(STR(?%s), %s)" var needle))]
                          (str "(" expr ")"))
              :is-null (format "(!BOUND(?%s))" var)
              :not-null (format "(BOUND(?%s))" var)
              nil)))))))

(defn- build-var-aliases
  "Map field-id to sanitized var name from the original column name."
  [field-ids]
  (let [aliases (into {}
                      (for [fid field-ids
                            :let [meta (field-id->metadata fid)
                                  nm   (or (:name meta) (str "f_" fid))]]
                        [fid (sanitize-var-name nm)]))]
    (log/debugf "[mbql] Built %d var aliases" (count aliases))
    aliases))

(defn- emit-optional-triple
  "Render a SPARQL `OPTIONAL { ?source <property> ?target . }` line.
   Single-arity defaults the source var to the synthetic subject (`?subject`)."
  ([property-uri target-var]
   (emit-optional-triple "subject" property-uri target-var))
  ([source-var property-uri target-var]
   (format "  OPTIONAL { ?%s <%s> ?%s . }" source-var property-uri target-var)))

(defn- joined-var-name
  "Build a SPARQL var name for a joined column: `<alias>__<field-name>`,
   sanitized. `field-name` may be nil; falls back to `f`."
  [alias field-name]
  (sanitize-var-name (str alias "__" (or field-name "f"))))

(defn- ensure-triple-for-field
  "Build OPTIONAL triple pattern for property and var."
  [property-uri var-alias]
  (let [triple (emit-optional-triple property-uri var-alias)]
    (log/debugf "[mbql] OPTIONAL triple: property=%s var=?%s" property-uri var-alias)
    triple))

(defn- compile-basic-filter
  "Compile a filter clause to one or more FILTER lines."
  [filter-clause field-id->var pair->target-var]
  (when-let [expr (compile-filter-expr filter-clause field-id->var pair->target-var)]
    [(str "  FILTER " expr)]))

;; ---------------------------------------------------------------------------
;; Custom expressions (Metabase "custom columns") → SPARQL
;; ---------------------------------------------------------------------------

(def ^:private xsd "http://www.w3.org/2001/XMLSchema#")

(defn- sparql-str-lit
  "Render `s` as a SPARQL string literal, escaping backslashes and double quotes
   (needed for regex patterns, which carry backslashes)."
  [s]
  (str "\"" (-> (str s)
                (str/replace "\\" "\\\\")
                (str/replace "\"" "\\\""))
       "\""))

(defn- regex-escape
  "Escape regex metacharacters so `s` matches literally inside a SPARQL REPLACE pattern."
  [s]
  (str/replace (str s) #"([\\.^$|?*+()\[\]{}])" "\\\\$1"))

(declare compile-expression)

(defn- expr-arg
  "Compile one argument of an expression to a SPARQL expression string: a literal,
   a `[:value v]` wrapper, a `[:field …]`/`[:expression …]` token (→ `?var`), or a
   nested operation. `resolve-token` maps a field/expression token to its var name."
  [arg resolve-token]
  (cond
    (number? arg)  (str arg)
    (string? arg)  (sparql-str-lit arg)
    (boolean? arg) (if arg "true" "false")
    (nil? arg)     "\"\""
    (and (vector? arg) (= :value (first arg)))      (expr-arg (second arg) resolve-token)
    (and (vector? arg) (#{:field :expression} (first arg)))
    (if-let [v (resolve-token arg)]
      (str "?" v)
      (throw (ex-info "Cannot resolve field/expression reference in expression"
                      {:token arg})))
    (sequential? arg) (compile-expression arg resolve-token)
    :else (throw (ex-info "Unsupported expression argument" {:arg arg}))))

(defn- compile-case
  "Compile a `[:case [[pred val]…] {:default d}]` clause to nested SPARQL `IF()`."
  [args resolve-token]
  (let [clauses (first args)
        opts    (second args)
        default (when (map? opts) (:default opts))
        a       #(expr-arg % resolve-token)
        b       #(compile-expression % resolve-token)]
    (reduce (fn [else [pred val]]
              (format "IF(%s, %s, %s)" (b pred) (a val) else))
            (if (some? default) (a default) "\"\"")
            (reverse clauses))))

(defn- compile-expression
  "Compile a Metabase expression clause to a SPARQL expression string. Supports the
   v1 function subset (arithmetic, string, regex, conditional, casts) plus the
   comparison/logical operators used inside `:case` predicates. Throws `ex-info`
   on an unsupported function so the query fails with a clear message rather than
   silently dropping the column."
  [clause resolve-token]
  (let [a #(expr-arg % resolve-token)
        s #(format "STR(%s)" (a %))
        cast (fn [iri x] (format "<%s%s>(%s)" xsd iri (a x)))]
    (if-not (sequential? clause)
      (a clause)
      (let [[op & args] clause]
        (case op
          :+ (str "(" (str/join " + " (map a args)) ")")
          :- (if (= 1 (count args))
               (str "(- " (a (first args)) ")")
               (str "(" (str/join " - " (map a args)) ")"))
          :* (str "(" (str/join " * " (map a args)) ")")
          :/ (str "(" (str/join " / " (map a args)) ")")
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
                     (format "REPLACE(%s, \"%s\", %s)"
                             (s txt)
                             (regex-escape find-str)
                             (sparql-str-lit (str/replace (str repl-str) "$" "\\$"))))
          (:regex-match-first :regexextract)
          (let [[txt pat] args
                pat-str (if (string? pat) pat (second pat))]
            (format "REPLACE(%s, %s, \"$1\")"
                    (s txt)
                    (sparql-str-lit (str "^.*?(" pat-str ").*$"))))
          (:float :double) (cast "double" (first args))
          :integer (cast "integer" (first args))
          :text    (format "STR(%s)" (a (first args)))
          ;; comparison / logical operators (used inside :case predicates)
          :=  (format "(%s = %s)"  (a (first args)) (a (second args)))
          :!= (format "(%s != %s)" (a (first args)) (a (second args)))
          :>  (format "(%s > %s)"  (a (first args)) (a (second args)))
          :>= (format "(%s >= %s)" (a (first args)) (a (second args)))
          :<  (format "(%s < %s)"  (a (first args)) (a (second args)))
          :<= (format "(%s <= %s)" (a (first args)) (a (second args)))
          :and (str "(" (str/join " && " (map a args)) ")")
          :or  (str "(" (str/join " || " (map a args)) ")")
          :not (str "(!" (a (first args)) ")")
          :case (compile-case args resolve-token)
          (throw (ex-info (str "Unsupported expression function: " op)
                          {:op op :clause clause})))))))

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
  "Compile a stage's `:expressions` map to SPARQL `BIND(… AS ?name)` lines.
   `token->var` resolves field/expression tokens to their SPARQL variable.
   Returns a vector of lines (one BIND per expression), in name order."
  [expressions token->var]
  (vec
   (for [[ename clause] (sort-by key expressions)]
     (str "  BIND(" (compile-expression clause token->var) " AS ?" (sanitize-var-name ename) ")"))))

(defn- filter-clause-value
  "Unwrap the RHS of a binary filter clause, stripping a `[:value v opts]` wrapper."
  [clause]
  (let [rhs (nth clause 2 nil)]
    (if (and (vector? rhs) (= :value (first rhs)))
      (second rhs)
      rhs)))

(defn- anchorable-clause?
  "True when `clause` is a `[:= <direct-field> <const>]` that can be pushed up into
   a mandatory BGP triple: a direct (no `:join-alias`) field with a resolvable
   property, not the synthetic subject, not a `langString` column (language
   equality semantics differ), and a non-nil concrete constant."
  [clause field-id->prop]
  (and (sequential? clause)
       (= := (first clause))
       (let [lhs   (second clause)
             fid   (field-token->id lhs)
             alias (field-token->join-alias lhs)
             v     (filter-clause-value clause)]
         (boolean
          (and fid
               (not alias)
               (not (id-field? fid))
               (not (lang-string-field? fid))
               (not (geometry-field? fid))
               (some? v)
               (contains? field-id->prop fid))))))

(defn- clause->anchor
  "Build the anchor map `{:fid :var :prop :term}` for an anchorable clause."
  [clause field-id->prop field-id->var]
  (let [fid (field-token->id (second clause))
        v   (filter-clause-value clause)]
    {:fid  fid
     :var  (get field-id->var fid)
     :prop (get field-id->prop fid)
     :term (value->term fid v)}))

(defn- extract-anchors
  "Pull pushable equality constraints out of `filter-clause`. Returns
   `{:anchors [{:fid :var :prop :term} …] :residual <clause-or-nil>}`.

   Anchors are taken from a top-level `:=` or from the `:=` children of a
   top-level `:and`; never from inside `:or`, where pushing would change
   semantics. `:residual` is `filter-clause` with the anchored clauses removed
   (an `:and` is unwrapped to its single remaining child, or `nil` when every
   child was pulled)."
  [filter-clause field-id->prop field-id->var]
  (cond
    (nil? filter-clause)
    {:anchors [] :residual nil}

    (anchorable-clause? filter-clause field-id->prop)
    {:anchors  [(clause->anchor filter-clause field-id->prop field-id->var)]
     :residual nil}

    (and (sequential? filter-clause) (= :and (first filter-clause)))
    (let [children (rest filter-clause)
          anchored (filter #(anchorable-clause? % field-id->prop) children)
          other    (remove #(anchorable-clause? % field-id->prop) children)]
      {:anchors  (mapv #(clause->anchor % field-id->prop field-id->var) anchored)
       :residual (cond
                   (empty? other)      nil
                   (= 1 (count other)) (first other)
                   :else               (vec (cons :and other)))})

    :else
    {:anchors [] :residual filter-clause}))

(defn- compile-order-by
  "Compile :order-by to ORDER BY."
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
  "Metabase's default result-column name for an aggregation clause — the name a
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
   token to its SPARQL variable. Returns nil for unsupported aggregations.

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
     (when expr
       {:select (format "(%s AS ?%s)" expr out)
        :var    out}))))

(defn- compile-agg-order-by
  "Compile `:order-by` for an aggregation query. Order terms may reference a
   breakout field (`[:field …]`) or an aggregation by index (`[:aggregation N]`)."
  [order-by field-id->var pair->target-var]
  (when (seq order-by)
    (let [parts (for [[dir tok & _] order-by
                      :let [v (cond
                                (and (vector? tok) (= :aggregation (first tok)))
                                (str "ag_" (second tok))
                                (and (vector? tok) (#{:field :expression} (first tok)))
                                (var-for-token tok field-id->var pair->target-var)
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

   Returns `{:vars [...] :triples [...]}`."
  [expected-cols {:keys [field-id->var pair->target-var alias->intermediate-var
                         fk-fid->alias default-graph]}]
  (let [placeholder (atom 0)]
    (reduce
     (fn [acc col]
       (let [fid      (:id col)
             alias    (or (:lib/join-alias col)
                          (when-let [fk-fid (:fk-field-id col)]
                            (get fk-fid->alias fk-fid)))
             existing (resolve-expected-var col field-id->var pair->target-var fk-fid->alias)]
         (cond
           existing
           (update acc :vars conj existing)

           ;; Joined column the compiler missed: bind it off the join's intermediate var.
           (and fid alias (get alias->intermediate-var alias))
           (let [inter (get alias->intermediate-var alias)]
             (if (id-field? fid)
               (update acc :vars conj inter)
               (let [nm   (:name (field-id->metadata fid))
                     v    (joined-var-name alias (or nm (str "f_" fid)))
                     prop (uri/absolute-uri nm default-graph)]
                 (-> acc
                     (update :vars conj v)
                     (update :triples conj
                             (emit-optional-triple inter prop v))))))

           ;; Direct column the compiler missed: bind it off ?subject.
           (and fid (not alias) (not (id-field? fid)) (:name (field-id->metadata fid)))
           (let [nm   (:name (field-id->metadata fid))
                 v    (sanitize-var-name nm)
                 prop (uri/absolute-uri nm default-graph)]
             (-> acc
                 (update :vars conj v)
                 (update :triples conj
                         (emit-optional-triple prop v))))

           (and fid (id-field? fid))
           (update acc :vars conj "subject")

           ;; Unresolvable column (e.g. an expression): project an unbound placeholder
           ;; so the column count still matches Lib; the value comes back as nil.
           :else
           (update acc :vars conj (str "undefined_" (swap! placeholder inc))))))
     {:vars [] :triples []}
     expected-cols)))

;; ---------------------------------------------------------------------------
;; Synthesized geometry coordinate columns (auto lon/lat, BOX corners)
;; ---------------------------------------------------------------------------
;; Sync stamps `:database-type "geo-coord:<axis>:<source>"` on driver-synthesized
;; coordinate columns (see `metabase.driver.sparql.database`). At compile time we
;; emit, instead of a property triple, the source geometry triple plus a
;; capture-group REPLACE that pulls the coordinate out and casts it to double.

(def ^:private wkt-point-regex
  "Capture lon ($1) and lat ($2) from a `POINT(lon lat)` lexical form."
  "^.*POINT *\\(([-0-9.eE+]+) ([-0-9.eE+]+).*")

(def ^:private wkt-box-regex
  "Capture minLon ($1) maxLon ($2) minLat ($3) maxLat ($4) from Virtuoso `BOX(...)`."
  "^.*BOX *\\(([-0-9.eE+]+) ([-0-9.eE+]+),([-0-9.eE+]+) ([-0-9.eE+]+).*")

(def ^:private geo-coord-axis->extract
  {"point-lon"   {:re wkt-point-regex :group 1}
   "point-lat"   {:re wkt-point-regex :group 2}
   "box-min-lon" {:re wkt-box-regex   :group 1}
   "box-max-lon" {:re wkt-box-regex   :group 2}
   "box-min-lat" {:re wkt-box-regex   :group 3}
   "box-max-lat" {:re wkt-box-regex   :group 4}})

(defn- geo-coord-marker
  "Parse a `geo-coord:<axis>:<source>` `:database-type` marker into
   `{:axis … :source …}`, or nil when the field is not a synthesized coordinate."
  [field-id]
  (let [dt (:database-type (field-id->metadata field-id))]
    (when (and (string? dt) (str/starts-with? dt "geo-coord:"))
      (let [[_ axis source] (str/split dt #":" 3)]
        {:axis axis :source source}))))

(defn- geo-coord-field?
  "True when the field is a synthesized geometry coordinate column."
  [field-id]
  (some? (geo-coord-marker field-id)))

(defn- geo-coord-emit
  "For a synthesized coordinate field `token`, return `{:triple :bind :var}`: the
   shared source geometry triple (off `?subject`, or the join intermediate var when
   the column is reached via a join) and the `BIND` that extracts + casts the
   coordinate. Returns nil when the axis or source cannot be resolved."
  [token {:keys [field-id->var pair->target-var alias->intermediate-var default-graph]}]
  (let [fid                   (field-token->id token)
        alias                 (field-token->join-alias token)
        {:keys [axis source]} (geo-coord-marker fid)
        {:keys [re group]}    (get geo-coord-axis->extract axis)
        coord-var             (if alias
                                (get pair->target-var [fid alias])
                                (get field-id->var fid))]
    (when (and re source coord-var)
      (let [src-prop (uri/absolute-uri source default-graph)
            src-var  (if alias (joined-var-name alias source) (sanitize-var-name source))
            triple   (if alias
                       (emit-optional-triple (get alias->intermediate-var alias) src-prop src-var)
                       (emit-optional-triple src-prop src-var))]
        {:triple triple
         :var    coord-var
         :bind   (format "  BIND(<%sdouble>(REPLACE(STR(?%s), %s, \"$%d\")) AS ?%s)"
                         xsd src-var (sparql-str-lit re) group coord-var)}))))

(defn- compile-base-stage
  "Compile a base MBQL stage (one with `:source-table`) to a SPARQL query.

   Left-joins (added by `add-implicit-joins` for FK-remap dimensions) are
   emitted as a pair of independent OPTIONALs:

     OPTIONAL { ?<src> <fk-prop> ?<alias>_subject . }
     OPTIONAL { ?<alias>_subject <target-prop> ?<alias>__<target-var> . }

   `?<src>` is `?subject` for joins reached directly from the source table.
   For chained implicit joins (e.g. Item → Provider → Owner), the FK field
   lives on a previously joined table; `alias->source-var` resolves `?<src>`
   to that prior join's intermediate var so the chain stays connected.

   Aggregation queries (`:aggregation` present) project only breakout columns
   and aggregate expressions, with a `GROUP BY` over the breakouts. `[:count]`
   compiles to `COUNT(DISTINCT ?subject)`.

   When `expected-cols` (Lib's authoritative column list) is supplied for a
   non-aggregation stage, the SELECT projection is reconciled against it so the
   driver's column count and order always match what the `annotate` middleware
   expects — see [[reconcile-base-projection]].

   Returns `{:sparql <query string> :vars <SELECT var names, in order>}`."
  [inner expected-cols]
  (let [limit         (:limit inner)
        table-id      (:source-table inner)
        class-uri     (table-id->class-uri table-id)
        default-graph (database-default-graph)
        fields        (:fields inner)
        order-by      (:order-by inner)
        filter-clause (:filter inner)
        joins         (:joins inner)
        aggregations  (:aggregation inner)
        breakout      (:breakout inner)
        expressions   (:expressions inner)
        agg?          (boolean (seq aggregations))
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
        ;; Joined-field references: `[field-id alias]` pairs that appear anywhere in the stage.
        joined-pairs   (collect-joined-pairs triple-inner)
        joined-fids    (set (map first joined-pairs))
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
                               [alias (uri/absolute-uri nm default-graph)]))
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
        ;; Per joined-pair: the SPARQL var that carries the value. The joined entity's
        ;; own subject column IS the intermediate var (no extra triple needed); every
        ;; other joined column gets a unique `<alias>__<field-name>` var.
        pair->target-var (into {}
                               (for [[fid alias] joined-pairs]
                                 [[fid alias]
                                  (if (id-field? fid)
                                    (get alias->intermediate-var alias)
                                    (joined-var-name alias
                                                     (or (:name (field-id->metadata fid))
                                                         (str "f_" fid))))]))
        ;; All field-ids referenced anywhere in the stage; we only build direct triples for
        ;; those that aren't reached via a join. Field tokens whose parent `:table-id`
        ;; isn't the base table are also excluded: they belong to a joined entity and
        ;; would otherwise emit a bogus `?subject <foreign-prop> ?var` triple. The
        ;; tolerated nil case keeps the test fixtures (no `:table-id`) working.
        field-ids     (->> (collect-field-ids triple-inner)
                           (remove joined-fids)
                           (remove (fn [fid]
                                     (when-let [tid (some-> fid field-id->metadata :table-id)]
                                       (not= tid table-id)))))
        field-id->prop (into {}
                             (for [fid field-ids
                                   :let [nm (:name (field-id->metadata fid))]
                                   :when (and nm (not (id-field? fid)) (not (geo-coord-field? fid)))]
                               [fid (uri/absolute-uri nm default-graph)]))
        field-id->var  (build-var-aliases field-ids)
        token->var     (fn [tok] (var-for-token tok field-id->var pair->target-var))
        ;; Synthesized coordinate columns referenced anywhere in the stage. Each
        ;; emits a shared source-geometry triple + an extraction BIND instead of a
        ;; property triple.
        geo-coord-tokens (let [walk (fn walk [x]
                                      (cond
                                        (and (vector? x) (= :field (first x))) [x]
                                        (sequential? x) (mapcat walk x)
                                        (map? x) (mapcat walk (vals x))
                                        :else []))]
                           (->> (concat output-tokens
                                        (mapcat (fn [[_dir fld & _]] [fld]) (or order-by []))
                                        (walk filter-clause)
                                        (collect-expression-tokens expressions))
                                (filter #(and (vector? %) (= :field (first %))
                                              (geo-coord-field? (field-token->id %))))
                                distinct))
        geo-coord-emits  (keep #(geo-coord-emit % {:field-id->var           field-id->var
                                                   :pair->target-var        pair->target-var
                                                   :alias->intermediate-var alias->intermediate-var
                                                   :default-graph           default-graph})
                               geo-coord-tokens)
        geo-coord-triples (distinct (map :triple geo-coord-emits))
        geo-coord-binds   (distinct (map :bind geo-coord-emits))
        ;; Push selective equality constraints into mandatory BGP triples (Principle 1):
        ;; instead of an OPTIONAL + trailing FILTER, anchor the constant directly so the
        ;; engine can do an index lookup. `:residual` is what's left for a bottom FILTER.
        {:keys [anchors residual]} (extract-anchors filter-clause field-id->prop field-id->var)
        anchored-fids  (set (keep :fid anchors))
        triples-for-fields (->> output-tokens
                                (keep (fn [tok]
                                        (let [fid   (field-token->id tok)
                                              alias (field-token->join-alias tok)]
                                          (when (and fid
                                                     (not alias)
                                                     (not (id-field? fid))
                                                     (not (contains? anchored-fids fid))
                                                     (get field-id->prop fid))
                                            (ensure-triple-for-field (get field-id->prop fid)
                                                                     (get field-id->var fid)))))))
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
                                 (remove joined-fids)
                                 (remove anchored-fids)
                                 (remove id-field?)))
        triples-for-extras (for [fid extra-direct-fids
                                 :let [prop (get field-id->prop fid)
                                       var  (get field-id->var fid)]
                                 :when (and prop var)]
                             (ensure-triple-for-field prop var))
        ;; OPTIONAL triples introduced by left-joins.
        join-fk-triples (for [j joins
                              :let [alias (:alias j)
                                    fk-prop (get alias->fk-prop alias)
                                    inter-var (get alias->intermediate-var alias)
                                    src-var (get alias->source-var alias "subject")]
                              :when (and fk-prop inter-var)]
                          (emit-optional-triple src-var fk-prop inter-var))
        ;; One triple per joined column. The joined entity's own subject column needs
        ;; no triple — it IS the intermediate var, already bound by the FK triple.
        join-target-triples (for [[fid alias] joined-pairs
                                  :when (and (not (id-field? fid))
                                             (not (geo-coord-field? fid)))
                                  :let [nm (:name (field-id->metadata fid))
                                        prop (uri/absolute-uri nm default-graph)
                                        target-var (get pair->target-var [fid alias])
                                        inter-var (get alias->intermediate-var alias)]
                                  :when (and prop target-var inter-var)]
                              (emit-optional-triple inter-var prop target-var))
        _ (log/debugf "[mbql] Triples: fields=%d extras=%d join-fk=%d join-targets=%d"
                      (count triples-for-fields) (count triples-for-extras)
                      (count join-fk-triples) (count join-target-triples))
        ;; LANG filters for `rdf:langString` columns, when a default-language is configured.
        ;; One per referenced variable; covers direct fields, extras, and joined targets.
        lang-filter-lines
        (let [lang (database-default-language)]
          (when-not (str/blank? lang)
            (let [direct-lang-vars (->> (concat (keep field-token->id (remove field-token->join-alias output-tokens))
                                                extra-direct-fids)
                                        (filter lang-string-field?)
                                        (keep #(get field-id->var %)))
                  joined-lang-vars (->> joined-pairs
                                        (filter (fn [[fid _]] (lang-string-field? fid)))
                                        (keep (fn [pair] (get pair->target-var pair))))]
              (mapv #(lang-filter-line % lang)
                    (distinct (concat direct-lang-vars joined-lang-vars))))))
        _ (log/debugf "[mbql] LANG filter lines: %d" (count (or lang-filter-lines [])))
        ;; Mandatory anchor triples (with the constant inlined) plus a BIND so the
        ;; projected/ordered column var stays available — see [[extract-anchors]].
        anchor-triples (for [{:keys [prop term]} anchors]
                         (format "  ?subject <%s> %s ." prop term))
        anchor-binds   (for [{:keys [term var]} anchors
                             :when var]
                         (format "  BIND(%s AS ?%s)" term var))
        _ (log/debugf "[mbql] Anchor triples: %d" (count anchor-triples))
        ;; Custom-column BINDs. Emitted after the triples that bind the variables
        ;; they reference (direct fields, extras, joined targets) so the values are
        ;; available; placed before filters/GROUP BY/ORDER BY which may use them.
        expr-bind-lines (compile-expressions expressions token->var)
        _ (log/debugf "[mbql] Expression BINDs: %d" (count expr-bind-lines))
        filters (when residual
                  (or (compile-basic-filter residual field-id->var pair->target-var)
                      []))
        _ (log/debugf "[mbql] Filters count: %d" (count filters))
        ;; --- SELECT / GROUP BY / ORDER BY ------------------------------------
        agg-projections (when agg?
                          (keep-indexed (fn [i a] (aggregation->projection a i token->var))
                                        aggregations))
        breakout-vars   (when agg?
                          (->> breakout (keep token->var) distinct vec))
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
                                  (keep (fn [tok]
                                          (when-let [a (field-token->join-alias tok)]
                                            (get pair->target-var [(field-token->id tok) a]))))
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
        ;; When Lib's expected columns are known, reconcile the SELECT against them so
        ;; the driver's column count/order can never drift from the `annotate` middleware.
        reconciled  (when (and expected-cols (not agg?))
                      (reconcile-base-projection
                       expected-cols
                       {:field-id->var           field-id->var
                        :pair->target-var        pair->target-var
                        :alias->intermediate-var alias->intermediate-var
                        :fk-fid->alias           fk-fid->alias
                        :default-graph           default-graph}))
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
                       (compile-agg-order-by order-by field-id->var pair->target-var)
                       (compile-order-by order-by field-id->var pair->target-var))
        where-body  (->> (concat [(format "  ?subject a <%s> ." class-uri)]
                                 anchor-triples
                                 anchor-binds
                                 triples-for-fields
                                 triples-for-extras
                                 join-fk-triples
                                 join-target-triples
                                 (or (:triples reconciled) [])
                                 geo-coord-triples
                                 geo-coord-binds
                                 expr-bind-lines
                                 (or lang-filter-lines [])
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
    {:sparql query
     :vars   result-vars}))

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

(defn- compile-derived-stage
  "Compile a derived MBQL stage (one with `:source-query`). The QP adds these
   wrapper stages when a saved card / model is used as a source, or when an FK
   display-value remap is layered on top of an aggregation.

   The inner stage is compiled as a SPARQL sub-`SELECT`; the derived stage's
   remap joins are emitted as OPTIONALs around it. The outer stage's own
   `:aggregation`, `:breakout`, `:filter`, `:order-by`, and explicit `:fields`
   projection are honored, resolved against the variables the sub-`SELECT`
   already projects (no triple patterns are needed at this level).

   When `expected-cols` (Lib's authoritative column list) is supplied for a
   non-aggregation stage, the SELECT projection is reconciled against it so the
   driver's column count and order always match what the `annotate` middleware
   expects.

   Returns `{:sparql … :vars …}`."
  [stage expected-cols]
  (let [default-graph (database-default-graph)
        inner         (compile-stage (:source-query stage))
        joins         (:joins stage)
        alias->join   (into {} (for [j joins] [(:alias j) j]))
        remap-entries (for [tok   (:fields stage)
                            :let  [alias (field-token->join-alias tok)]
                            :when alias
                            :let  [tid    (field-token->id tok)
                                   join   (get alias->join alias)
                                   fk-var (some-> join :condition condition->fk-ref inner-var-for-ref)
                                   nm     (when (integer? tid) (:name (field-id->metadata tid)))
                                   prop   (when nm (uri/absolute-uri nm default-graph))
                                   rvar   (joined-var-name alias (or nm (str "f_" tid)))]
                            :when (and fk-var prop)]
                        {:optional (emit-optional-triple fk-var prop rvar)
                         :var      rvar
                         :tid      tid
                         :alias    alias})
        ;; Columns visible to the outer stage: inner sub-SELECT vars + remapped vars.
        passthrough-vars (vec (concat (:vars inner) (map :var remap-entries)))
        ;; The sub-SELECT already projects these by name, so resolving an outer
        ;; field token (a string-named source-query column) is just sanitizing it.
        ;; Aggregation results are the exception: the inner stage names them `ag_N`,
        ;; but a later stage references them by Lib's name (`count`, `sum`, …), so we
        ;; add those aliases (drilling on an aggregation value relies on this).
        field-id->var    (merge (into {} (for [v passthrough-vars] [v (sanitize-var-name v)]))
                                (aggregation-name->var (:aggregation (:source-query stage))))
        pair->target-var (into {} (for [{:keys [tid alias var]} remap-entries]
                                    [[tid alias] var]))
        token->var       (fn [tok] (var-for-token tok field-id->var pair->target-var))
        aggregations  (:aggregation stage)
        breakout      (:breakout stage)
        agg?          (boolean (seq aggregations))
        filter-clause (:filter stage)
        order-by      (:order-by stage)
        limit         (:limit stage)
        agg-projections (when agg?
                          (vec (keep-indexed (fn [i a] (aggregation->projection a i token->var true))
                                             aggregations)))
        breakout-vars   (when agg?
                          (->> breakout (keep token->var) distinct vec))
        ;; Non-agg explicit projection: resolve every :fields token; fall back to
        ;; passthrough if any token cannot be resolved.
        projected-vars  (when (and (not agg?) (seq (:fields stage)))
                          (let [vs (map token->var (:fields stage))]
                            (when (every? some? vs)
                              (vec (distinct vs)))))
        ;; When Lib's expected columns are known, reconcile the SELECT against them:
        ;; remap columns resolve via `pair->target-var` (or are synthesized as an extra
        ;; OPTIONAL), every other column consumes the next inner sub-SELECT var in order.
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
                                       prop (uri/absolute-uri nm default-graph)]
                                   (-> acc
                                       (update :vars conj rvar)
                                       (update :optionals conj
                                               (emit-optional-triple fk-var prop rvar))))

                                 (seq @inner-vars)
                                 (let [v (first @inner-vars)]
                                   (swap! inner-vars rest)
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
        ;; sub-SELECT's columns (by Lib name too) and any remap vars.
        expressions   (:expressions stage)
        expr-token->var (fn [tok] (var-for-token tok outer-field-id->var pair->target-var))
        expr-bind-lines (compile-expressions expressions expr-token->var)
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
                        (compile-agg-order-by order-by outer-field-id->var pair->target-var)
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
                           (when (seq filters)
                             (str (str/join "\n" filters) "\n"))
                           "}\n"
                           (when group-by-clause (str group-by-clause "\n"))
                           (when order-clause (str order-clause "\n"))
                           (when (number? limit) (str "LIMIT " limit)))]
    (log/debugf "[sparql.mbql] Compiled derived stage: %s" query)
    {:sparql query
     :vars   result-vars}))

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
  "Lib's authoritative result columns for `outer-query` (pMBQL) — the same calculation
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

   Multi-stage queries (a saved card / model used as a source, or an FK
   display-value remap layered on an aggregation) are compiled stage by stage:
   the inner stage becomes a SPARQL sub-`SELECT` and the outer stage's remap
   joins wrap it. See [[compile-base-stage]] for the per-stage details."
  [_driver outer-query]
  (let [expected-cols    (expected-result-columns outer-query)
        legacy-query     (driver-api/->legacy-MBQL outer-query)
        {:keys [sparql vars]} (compile-stage (:query legacy-query) expected-cols)]
    (log/debugf "[sparql.mbql->native] Compiled query (%d projected columns): %s"
                (count vars) sparql)
    {:query sparql
     :mbql? true}))


