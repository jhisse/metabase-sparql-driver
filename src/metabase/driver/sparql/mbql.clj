(ns metabase.driver.sparql.mbql
  "Simple MBQL → SPARQL transpilation. No aggregations or complex functions."
  (:require
   [clojure.string :as str]
   [clojure.set :as set]
   [metabase.driver-api.core :as driver-api]
   [metabase.query-processor.store :as qp.store]
   [metabase.lib.metadata :as lib.metadata]
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

(defn- field-id->metadata
  "Resolve field metadata by ID."
  [field-id]
  (when field-id
    (lib.metadata/field (qp.store/metadata-provider) field-id)))

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
  (some-> (lib.metadata/database (qp.store/metadata-provider))
          :details
          :default-graph))

(defn- database-default-language
  "Read the Default Language (BCP-47 tag) from connection details. May be blank."
  []
  (some-> (lib.metadata/database (qp.store/metadata-provider))
          :details
          :default-language))

(defn- lang-string-field?
  "True when the field's `:database-type` indicates an `rdf:langString` property
   (set during SHACL sync). Other sync strategies always emit `\"string\"`, so
   this is effectively a SHACL-only signal."
  [field-id]
  (= "langString" (:database-type (field-id->metadata field-id))))

(defn- lang-filter-line
  "Render the LANG filter clause for one variable. Guards against unbound
   variables (left joins) and accepts untagged literals alongside the target
   language."
  [var-name lang]
  (format "  FILTER(!BOUND(?%s) || LANG(?%s) = \"%s\" || LANG(?%s) = \"\")"
          var-name var-name lang var-name))

(defn- absolute-uri
  "Reconstruct a full URI from a (possibly shortened) name.
   If `nm` already has a URI scheme, returns it unchanged; otherwise prepends
   `default-graph` (the implicit base prefix). Blank `default-graph` is a no-op."
  [nm default-graph]
  (cond
    (str/blank? nm) nm
    (re-find #"^[A-Za-z][A-Za-z0-9+.-]*:" nm) nm
    (str/blank? default-graph) nm
    :else (str default-graph nm)))

(defn- table-id->class-uri
  "Resolve RDF class URI (table name) from :source-table."
  [table-id]
  (let [nm  (some-> (lib.metadata/table (qp.store/metadata-provider) table-id)
                    :name)
        uri (absolute-uri nm (database-default-graph))]
    (log/debugf "[mbql] Resolved class URI for table-id %s: %s" table-id uri)
    uri))

(defn- collect-field-ids
  "Collect referenced field IDs from fields/order-by/filter."
  [{:keys [fields order-by] filter-clause :filter}]
  (let [ids-from-fields (set (keep field-token->id fields))
        ids-from-order  (set (keep (fn [[_dir fld & _]] (field-token->id fld)) order-by))
        ids-from-filter (letfn [(collect-field-tokens [x]
                                  (cond
                                    (and (vector? x) (= :field (first x))) [x]
                                    (sequential? x) (mapcat collect-field-tokens x)
                                    (map? x) (mapcat collect-field-tokens (vals x))
                                    :else []))]
                          (set (keep field-token->id (collect-field-tokens filter-clause))))
        all-ids         (vec (set/union ids-from-fields ids-from-order ids-from-filter))]
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
  (let [fid   (field-token->id field-token)
        alias (field-token->join-alias field-token)]
    (cond
      (and fid alias) (get pair->target-var [fid alias])
      (and fid (id-field? fid)) "subject"
      fid (get field-id->var fid))))

(defn- condition->fk-field-id
  "Extract the *current-table* field-id from a join `:condition`. The condition
   is a legacy-MBQL filter clause, normally `[:= [:field A] [:field B {:join-alias …}]]`
   (a `[:and …]` wrapper for multi-column joins is unwrapped to its first `:=`).
   The current-table side is the `:field` token *without* a `:join-alias`."
  [condition]
  (when (sequential? condition)
    (let [eq (cond
               (= := (first condition)) condition
               (= :and (first condition)) (first (filter #(and (sequential? %) (= := (first %)))
                                                          (rest condition)))
               :else nil)]
      (when eq
        (->> (rest eq)
             (filter #(and (vector? %) (= :field (first %))))
             (remove field-token->join-alias)
             (keep field-token->id)
             first)))))

(defn- collect-joined-pairs
  "Walk a legacy-MBQL stage and return the set of `[field-id alias]` pairs for every
   `[:field id {:join-alias \"...\"}]` token that appears in `:fields`, `:order-by`,
   or `:filter`."
  [{:keys [fields order-by] filter-clause :filter}]
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
        (let [fid (field-token->id lhs)
              var (var-for-token lhs field-id->var pair->target-var)
              opts (when (map? maybe-opts) maybe-opts)
              insensitive? (false? (:case-sensitive opts))
              v (let [x rhs]
                  (if (and (vector? x) (= :value (first x)))
                    (second x)
                    x))]
          (when (and fid var)
            (case op
              := (if (nil? v)
                   (format "(!BOUND(?%s))" var)
                   (format "(?%s = %s)" var (literal->sparql v)))
              :!= (if (nil? v)
                    (format "(BOUND(?%s))" var)
                    (format "(?%s != %s)" var (literal->sparql v)))
              :> (and (some? v) (format "(?%s > %s)" var (literal->sparql v)))
              :>= (and (some? v) (format "(?%s >= %s)" var (literal->sparql v)))
              :< (and (some? v) (format "(?%s < %s)" var (literal->sparql v)))
              :<= (and (some? v) (format "(?%s <= %s)" var (literal->sparql v)))
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

(defn- ensure-triple-for-field
  "Build OPTIONAL triple pattern for property and var."
  [property-uri var-alias]
  (let [triple (format "  OPTIONAL { ?subject <%s> ?%s . }" property-uri var-alias)]
    (log/debugf "[mbql] OPTIONAL triple: property=%s var=?%s" property-uri var-alias)
    triple))

(defn- compile-basic-filter
  "Compile a filter clause to one or more FILTER lines."
  [filter-clause field-id->var pair->target-var]
  (when-let [expr (compile-filter-expr filter-clause field-id->var pair->target-var)]
    [(str "  FILTER " expr)]))

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
  "Return the `[:field …]` token an aggregation operates on, or nil for arg-less
   aggregations such as `[:count]`."
  [agg]
  (let [agg (unwrap-aggregation agg)
        arg (when (sequential? agg) (second agg))]
    (when (and (vector? arg) (= :field (first arg)))
      arg)))

(defn- aggregation->projection
  "Compile one aggregation clause to a SPARQL projection
   `{:select \"(EXPR AS ?ag_N)\" :var \"ag_N\"}`. `token->var` resolves a field
   token to its SPARQL variable. Returns nil for unsupported aggregations.

   `[:count]` with no argument becomes `COUNT(DISTINCT ?subject)` so that the
   multi-valued OPTIONAL fan-out does not inflate entity counts."
  [agg index token->var]
  (let [agg     (unwrap-aggregation agg)
        op      (when (sequential? agg) (first agg))
        out     (str "ag_" index)
        arg     (aggregation-arg-token agg)
        arg-var (when arg (token->var arg))
        expr    (case op
                  :count    (if arg-var
                              (format "COUNT(?%s)" arg-var)
                              "COUNT(DISTINCT ?subject)")
                  :distinct (when arg-var (format "COUNT(DISTINCT ?%s)" arg-var))
                  :sum      (when arg-var (format "SUM(?%s)" arg-var))
                  :avg      (when arg-var (format "AVG(?%s)" arg-var))
                  :min      (when arg-var (format "MIN(?%s)" arg-var))
                  :max      (when arg-var (format "MAX(?%s)" arg-var))
                  nil)]
    (when expr
      {:select (format "(%s AS ?%s)" expr out)
       :var    out})))

(defn- compile-agg-order-by
  "Compile `:order-by` for an aggregation query. Order terms may reference a
   breakout field (`[:field …]`) or an aggregation by index (`[:aggregation N]`)."
  [order-by field-id->var pair->target-var]
  (when (seq order-by)
    (let [parts (for [[dir tok & _] order-by
                      :let [v (cond
                                (and (vector? tok) (= :aggregation (first tok)))
                                (str "ag_" (second tok))
                                (and (vector? tok) (= :field (first tok)))
                                (var-for-token tok field-id->var pair->target-var)
                                :else nil)]
                      :when v]
                  (str (str/upper-case (name dir)) "(?" v ")"))]
      (when (seq parts)
        (str "ORDER BY " (str/join " " parts))))))

(defn mbql->native
  "Compile outer MBQL to a native SPARQL map.

   Metabase passes pMBQL (Lib / MBQL 5) here; convert to legacy MBQL first
   so the rest of the compiler can read `:query`, `:source-table`, `:fields`,
   `:filter`, `:order-by`, `:limit` directly.

   Left-joins (added by `add-implicit-joins` for FK-remap dimensions) are
   emitted as a pair of independent OPTIONALs:

     OPTIONAL { ?subject <fk-prop> ?<alias>_subject . }
     OPTIONAL { ?<alias>_subject <target-prop> ?<alias>__<target-var> . }

   That matches SQL LEFT JOIN semantics (rows with no FK still appear; rows
   with FK but no target value still appear). Multi-valued FKs fan out
   naturally — SPARQL produces one solution per matching target.

   Aggregation queries (`:aggregation` present) take a different SELECT shape:
   only breakout columns and aggregate expressions are projected, with a
   `GROUP BY` over the breakout columns. `[:count]` compiles to
   `COUNT(DISTINCT ?subject)` so the multi-valued OPTIONAL fan-out does not
   inflate entity counts. `:sum` / `:avg` / `:min` / `:max` / `:distinct`
   compile to the matching SPARQL aggregate over the column variable."
  [_driver outer-query]
  (let [outer-query   (driver-api/->legacy-MBQL outer-query)
        inner         (:query outer-query)
        table-id      (:source-table inner)
        class-uri     (table-id->class-uri table-id)
        default-graph (database-default-graph)
        fields        (:fields inner)
        order-by      (:order-by inner)
        limit         (:limit inner)
        filter-clause (:filter inner)
        joins         (:joins inner)
        aggregations  (:aggregation inner)
        breakout      (:breakout inner)
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
                                                   (condition->fk-field-id (:condition j)))
                                         nm    (when fk-id (:name (field-id->metadata fk-id)))]
                                   :when nm]
                               [alias (absolute-uri nm default-graph)]))
        ;; Per joined-pair: the SPARQL var that carries the value. The joined entity's
        ;; own subject column IS the intermediate var (no extra triple needed); every
        ;; other joined column gets a unique `<alias>__<field-name>` var.
        pair->target-var (into {}
                               (for [[fid alias] joined-pairs]
                                 [[fid alias]
                                  (if (id-field? fid)
                                    (get alias->intermediate-var alias)
                                    (sanitize-var-name
                                     (str alias "__" (or (:name (field-id->metadata fid))
                                                          (str "f_" fid)))))]))
        ;; All field-ids referenced anywhere in the stage; we only build direct triples for
        ;; those that aren't reached via a join.
        field-ids     (collect-field-ids triple-inner)
        field-id->prop (into {}
                             (for [fid field-ids
                                   :let [nm (:name (field-id->metadata fid))]
                                   :when (and nm (not (id-field? fid)))]
                               [fid (absolute-uri nm default-graph)]))
        field-id->var  (build-var-aliases field-ids)
        token->var     (fn [tok] (var-for-token tok field-id->var pair->target-var))
        triples-for-fields (->> output-tokens
                                (keep (fn [tok]
                                        (let [fid   (field-token->id tok)
                                              alias (field-token->join-alias tok)]
                                          (when (and fid
                                                     (not alias)
                                                     (not (id-field? fid))
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
                                         (collect-field-tokens filter-clause))
                                 (remove field-token->join-alias)
                                 (keep field-token->id)
                                 set
                                 (remove (set (keep field-token->id (remove field-token->join-alias output-tokens))))
                                 (remove joined-fids)
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
                                    inter-var (get alias->intermediate-var alias)]
                              :when (and fk-prop inter-var)]
                          (format "  OPTIONAL { ?subject <%s> ?%s . }" fk-prop inter-var))
        ;; One triple per joined column. The joined entity's own subject column needs
        ;; no triple — it IS the intermediate var, already bound by the FK triple.
        join-target-triples (for [[fid alias] joined-pairs
                                  :when (not (id-field? fid))
                                  :let [nm (:name (field-id->metadata fid))
                                        prop (absolute-uri nm default-graph)
                                        target-var (get pair->target-var [fid alias])
                                        inter-var (get alias->intermediate-var alias)]
                                  :when (and prop target-var inter-var)]
                              (format "  OPTIONAL { ?%s <%s> ?%s . }" inter-var prop target-var))
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
        filters (when filter-clause
                  (or (compile-basic-filter filter-clause field-id->var pair->target-var)
                      []))
        _ (log/debugf "[mbql] Filters count: %d" (count filters))
        ;; --- SELECT / GROUP BY / ORDER BY ------------------------------------
        agg-projections (when agg?
                          (keep-indexed (fn [i a] (aggregation->projection a i token->var))
                                        aggregations))
        breakout-vars   (when agg?
                          (->> breakout (keep token->var) distinct vec))
        select-part (if agg?
                      ;; Aggregation query: project breakout columns then aggregate
                      ;; expressions. No ?subject in the projection.
                      (str "SELECT "
                           (str/join " " (concat (map #(str "?" %) breakout-vars)
                                                 (map :select agg-projections))))
                      ;; Plain SELECT: ?subject + direct fields + joined target vars.
                      (let [direct-select-vars (->> fields
                                                    (remove field-token->join-alias)
                                                    (keep field-token->id)
                                                    (remove id-field?)
                                                    (map field-id->var)
                                                    (remove nil?)
                                                    distinct
                                                    vec)
                            joined-select-vars (->> fields
                                                    (keep (fn [tok]
                                                            (when-let [a (field-token->join-alias tok)]
                                                              (get pair->target-var [(field-token->id tok) a]))))
                                                    distinct
                                                    vec)
                            select-vars (concat direct-select-vars joined-select-vars)]
                        (str "SELECT ?subject"
                             (when (seq select-vars)
                               (str " " (str/join " " (map #(str "?" %) select-vars)))))))
        group-by-clause (when (and agg? (seq breakout-vars))
                          (str "GROUP BY " (str/join " " (map #(str "?" %) breakout-vars))))
        order-clause (if agg?
                       (compile-agg-order-by order-by field-id->var pair->target-var)
                       (compile-order-by order-by field-id->var pair->target-var))
        where-body  (->> (concat [(format "  ?subject a <%s> ." class-uri)]
                                 triples-for-fields
                                 triples-for-extras
                                 join-fk-triples
                                 join-target-triples
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
    (log/debugf "[sparql.mbql->native] Compiled query: %s" query)
    {:query query
     :mbql? true}))


