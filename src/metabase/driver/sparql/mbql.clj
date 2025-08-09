(ns metabase.driver.sparql.mbql
  "Simple MBQL → SPARQL transpilation. No aggregations or complex functions."
  (:require
   [clojure.string :as str]
   [clojure.set :as set]
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

(defn- field-id->metadata
  "Resolve field metadata by ID."
  [field-id]
  (when field-id
    (lib.metadata/field (qp.store/metadata-provider) field-id)))

(defn- id-field?
  "Return true if the field-id represents the subject (id) column."
  [field-id]
  (let [m (field-id->metadata field-id)]
    (or (= "id" (:name m))
        (:pk? m))))

(defn- table-id->class-uri
  "Resolve RDF class URI (table name) from :source-table."
  [table-id]
  (let [uri (some-> (lib.metadata/table (qp.store/metadata-provider) table-id)
                    :name)]
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

(defn- var-for-field-id
  "Return SPARQL var for a field-id (handles subject)."
  [field-id field-id->var]
  (if (id-field? field-id) "subject" (get field-id->var field-id)))

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
  [filter-clause field-id->var]
  (when (sequential? filter-clause)
    (let [[op lhs rhs maybe-opts & more] filter-clause]
      (case op
        :and (let [parts (->> (concat [lhs rhs] more)
                              (keep #(compile-filter-expr % field-id->var)))]
               (when (seq parts)
                 (str "(" (str/join " && " parts) ")")))
        :or  (let [parts (->> (concat [lhs rhs] more)
                              (keep #(compile-filter-expr % field-id->var)))]
               (when (seq parts)
                 (str "(" (str/join " || " parts) ")")))
        :not (when-let [inner (compile-filter-expr lhs field-id->var)]
               (str "(!" inner ")"))
        (let [fid (field-token->id lhs)
              var (some-> fid (var-for-field-id field-id->var))
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
  (let [triple (format "  OPTIONAL { ?s <%s> ?%s . }" property-uri var-alias)]
    (log/debugf "[mbql] OPTIONAL triple: property=%s var=?%s" property-uri var-alias)
    triple))

(defn- compile-basic-filter
  "Compile a filter clause to one or more FILTER lines."
  [filter-clause field-id->var _field-id->prop]
  (when-let [expr (compile-filter-expr filter-clause field-id->var)]
    [(str "  FILTER " expr)]))

(defn- compile-order-by
  "Compile :order-by to ORDER BY."
  [order-by field-id->var]
  (when (seq order-by)
    (let [parts (for [[dir fld & _] order-by
                      :let [fid (field-token->id fld)
                            var (if (id-field? fid) "subject" (some-> fid field-id->var))]]
                  (when var
                    (str (str/upper-case (name dir)) "(?" var ")")))
          parts (remove nil? parts)]
      (when (seq parts)
        (let [clause (str "ORDER BY " (str/join " " parts))]
          (log/debugf "[mbql] Order clause: %s" clause)
          clause)))) )

(defn- ensure-binding-id
  "Expose subject as ?subject."
  []
  "  BIND(?s AS ?subject) .")

(defn mbql->native
  "Compile outer MBQL to a native SPARQL map."
  [_driver outer-query]
  (let [inner        (:query outer-query)
        table-id     (:source-table inner)
        class-uri    (table-id->class-uri table-id)
        fields       (:fields inner)
        order-by     (:order-by inner)
        limit        (:limit inner)
        filter-clause (:filter inner)
        _            (log/debugf "[mbql] Start compile: table-id=%s fields=%d order-by=%d limit=%s has-filter=%s"
                                 table-id (count fields) (count order-by) (pr-str limit) (boolean filter-clause))
        field-ids    (collect-field-ids inner)
        field-id->prop (into {}
                              (for [fid field-ids
                                    :let [nm (:name (field-id->metadata fid))]
                                    :when (and nm (not (id-field? fid)))]
                                [fid nm]))
        field-id->var  (build-var-aliases field-ids)
        triples-for-fields (->> fields
                                (keep field-token->id)
                                (keep (fn [fid]
                                        (when-let [prop (and (not (id-field? fid))
                                                             (get field-id->prop fid))]
                                          (ensure-triple-for-field prop (get field-id->var fid))))))
        extra-field-ids (letfn [(collect-field-tokens [x]
                                  (cond
                                    (and (vector? x) (= :field (first x))) [x]
                                    (sequential? x) (mapcat collect-field-tokens x)
                                    (map? x) (mapcat collect-field-tokens (vals x))
                                    :else []))]
                          (->> (concat (map #(nth % 1 nil) (or order-by []))
                                       (collect-field-tokens filter-clause))
                               (keep field-token->id)
                               set
                               (remove (set (keep field-token->id fields)))))
        triples-for-extras (for [fid extra-field-ids
                                 :let [prop (get field-id->prop fid)
                                       var  (get field-id->var fid)]
                                 :when prop]
                             (ensure-triple-for-field prop var))
        _                 (log/debugf "[mbql] Triples: fields=%d extras=%d" (count triples-for-fields) (count triples-for-extras))
        filters (when filter-clause
                  (or (compile-basic-filter filter-clause field-id->var field-id->prop)
                      []))
        _       (log/debugf "[mbql] Filters count: %d" (count filters))
        order-clause (compile-order-by order-by field-id->var)
        select-vars (->> (keep field-token->id fields)
                         (remove id-field?)
                         (map field-id->var)
                         (remove nil?)
                         distinct
                         vec)
        select-part (str "SELECT ?subject"
                         (when (seq select-vars)
                           (str " " (str/join " " (map #(str "?" %) select-vars)))))
        where-body  (->> (concat [(format "  ?s a <%s> ." class-uri)
                                   (ensure-binding-id)]
                                  triples-for-fields
                                  triples-for-extras
                                  filters)
                          (str/join "\n"))
        where-part  (str "WHERE {\n" where-body "\n}")
        limit-part  (when (number? limit) (str "LIMIT " limit))
        query       (str (str/trim select-part) "\n"
                         where-part "\n"
                         (when order-clause (str order-clause "\n"))
                         (when limit-part (str limit-part)))]
    (log/debugf "[sparql.mbql->native] Compiled query: %s" query)
    {:query query
     :mbql? true}))


