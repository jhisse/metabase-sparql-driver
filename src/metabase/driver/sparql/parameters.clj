(ns metabase.driver.sparql.parameters
  "SPARQL Parameter Substitution for Metabase SPARQL Driver.

   Replaces `{{tag}}` placeholders in native SPARQL queries with their parameter
   values, rendering each value as a syntactically correct SPARQL term:

     - strings → `\"escaped\"`
     - IRIs (`http(s)://` or `urn:` values) → `<value>`
     - numbers / booleans → bare literal
     - dates → `\"2024-01-15\"^^xsd:date` (`xsd:dateTime` when a time is given)
     - sequential collections → comma-separated SPARQL terms (only valid inside
       `IN(...)` / `VALUES`; template authors must wrap accordingly)
     - `[[ … ]]` clauses → dropped when one of their parameters has no value;
       anywhere else a `{{…}}` without a value stays as written, which keeps a
       commented-out tag (`# … {{x}}`) intact"
  (:require
   [clojure.string :as str]
   [metabase.driver-api.core :as driver-api]
   [metabase.driver.common.parameters :as params]
   ^{:clj-kondo/ignore [:deprecated-namespace]} [metabase.driver.common.parameters.parse :as params.parse]
   [metabase.driver.common.parameters.values :as params.values]
   [metabase.driver.sparql.uri :as uri]
   [metabase.util.log :as log]))

(defn- unsupported-kind
  "The name of a parameter value record we cannot meaningfully render in SPARQL,
   or nil: Field Filters (SQL-shaped BETWEEN/IN clauses), referenced cards,
   snippets, and referenced tables. Predicate fns live in
   `metabase.driver.common.parameters` itself precisely so callers don't need
   to import each record class."
  [v]
  (cond
    (params/FieldFilter? v)            "Field Filter"
    (params/ReferencedCardQuery? v)    "saved question"
    (params/ReferencedQuerySnippet? v) "snippet"
    (params/ReferencedTableQuery? v)   "table"))

(defn- record-value
  "Pull the underlying scalar(s) out of a Metabase parameter value record.
   Returns the value unchanged when `v` isn't a record we recognize."
  [v]
  (cond
    (unsupported-kind v)
    (throw (ex-info (format "The SPARQL driver does not support %s variables; use a Text, Number or Date variable."
                            (unsupported-kind v))
                    {:type driver-api/qp.error-type.unsupported-feature}))
    ;; DateRange / DateTimeRange — render as ISO "start/end".
    (or (instance? metabase.driver.common.parameters.DateRange v)
        (instance? metabase.driver.common.parameters.DateTimeRange v))
    (str (:start v) "/" (:end v))
    ;; TemporalUnit wraps a raw scalar in `:value`.
    ;; Plain map shape `{:type … :value …}` occasionally surfaces.
    (or (params/TemporalUnit? v)
        (and (map? v) (contains? v :value)))
    (:value v)
    :else                                v))

(defn- date-literal
  "A Date parameter's `s` as a typed literal: a plain string never compares
   equal to, or orders against, an xsd:date value. xsd:dateTime needs seconds,
   which a `…THH:mm` value lacks."
  [s]
  (str (uri/string-literal (cond-> s (re-find #"T\d{2}:\d{2}$" s) (str ":00")))
       "^^<http://www.w3.org/2001/XMLSchema#" (if (str/includes? s "T") "dateTime" "date") ">"))

(declare ->sparql-term)

(defn- ->sparql-term
  "Render a single parameter value as a SPARQL term. Returns nil when the value
   is `no-value` / nil, which callers treat as a missing value."
  [v]
  (let [v (record-value v)]
    (cond
      (instance? metabase.driver.common.parameters.Date v) (date-literal (:s v))
      (or (nil? v) (= params/no-value v)) nil
      (sequential? v)       (let [terms (keep ->sparql-term v)]
                              (when (seq terms)
                                (str/join ", " terms)))
      (or (boolean? v) (number? v)) (str v)
      (uri/iri-shaped? v)   (uri/iri-ref v)
      (string? v)           (uri/string-literal v)
      :else                 (do (log/warnf "[sparql.params] Unrecognized parameter value class %s; falling back to a string literal"
                                           (class v))
                                (uri/string-literal v)))))

(defn- substitute
  "Render parsed query tokens (strings, `Param`s, `Optional`s) as
   `[fragments missing]`: the query fragments, and the names of parameters
   without a value, which stay as written. An `[[ … ]]` clause is dropped whole
   when any of its parameters is missing."
  [param->value tokens]
  (reduce
   (fn [[acc missing] token]
     (cond
       (string? token)         [(conj acc token) missing]
       (params/Param? token)   (if-let [term (->sparql-term (get param->value (:k token)))]
                                 [(conj acc term) missing]
                                 [(conj acc (str "{{" (:k token) "}}")) (conj missing (:k token))])
       (params/Optional? token) (let [[opt opt-missing] (substitute param->value (:args token))]
                                  [(cond-> acc (empty? opt-missing) (into opt)) missing])
       :else                   (throw (ex-info (str "The SPARQL driver cannot substitute " (pr-str token))
                                               {:type driver-api/qp.error-type.unsupported-feature}))))
   [[] []]
   tokens))

(defn substitute-native-parameters
  "Substitute `{{tag}}` placeholders in `inner-query`'s `:query` string using
   the parameters / template-tags in `inner-query`, and drop `[[ … ]]` clauses
   whose parameters have no value. Returns the updated inner-query map."
  [_driver inner-query]
  #_{:clj-kondo/ignore [:unresolved-var]}
  (let [param->value      (params.values/query->params-map inner-query)
        ;; false: SPARQL comments are `#`, which the parser does not know;
        ;; a tag in one is left as written when it has no value.
        [parts missing]   (substitute param->value (params.parse/parse (:query inner-query) false))]
    (when (seq missing)
      (log/warnf "[sparql.params] No value for %s; left as written" (vec (distinct missing))))
    (let [substituted (str/join parts)]
      (log/debugf "[sparql.params] Substituted query: %s" substituted)
      (assoc inner-query :query substituted))))
