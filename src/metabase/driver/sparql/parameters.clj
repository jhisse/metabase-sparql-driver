(ns metabase.driver.sparql.parameters
  "SPARQL Parameter Substitution for Metabase SPARQL Driver.

   Replaces `{{tag}}` placeholders in native SPARQL queries with their parameter
   values, rendering each value as a syntactically correct SPARQL term:

     - strings → `\"escaped\"`
     - IRIs (URLs with a scheme) → `<value>`
     - numbers / booleans → bare literal
     - sequential collections → comma-separated SPARQL terms (only valid inside
       `IN(...)` / `VALUES`; template authors must wrap accordingly)
     - missing/optional values → placeholder is left untouched and a warning logged"
  (:require
   [clojure.string :as str]
   [metabase.driver.common.parameters :as params]
   [metabase.driver.common.parameters.values :as params.values]
   [metabase.util.log :as log])
  (:import
   (java.util.regex Matcher Pattern)))

(defn- iri-shaped?
  "Heuristic: a string that looks like an absolute IRI we should wrap in `<...>`.
   We accept `http(s)://...` and `urn:...` shapes; anything else stays a literal."
  [s]
  (boolean
   (when (string? s)
     (re-find #"^(?:https?://|urn:)" s))))

(defn- escape-sparql-string
  "Escape characters that would break a SPARQL double-quoted string literal."
  [s]
  (-> s
      (str/replace "\\" "\\\\")
      (str/replace "\"" "\\\"")
      (str/replace "\n" "\\n")
      (str/replace "\r" "\\r")
      (str/replace "\t" "\\t")))

(defn- unsupported-record?
  "True for parameter value records we cannot meaningfully render in SPARQL:
   FieldFilter (SQL-shaped BETWEEN/IN clauses), referenced cards, snippets, and
   referenced tables. Predicate fns live in `metabase.driver.common.parameters`
   itself precisely so callers don't need to import each record class."
  [v]
  (or (params/FieldFilter? v)
      (params/ReferencedCardQuery? v)
      (params/ReferencedQuerySnippet? v)
      (params/ReferencedTableQuery? v)))

(defn- record-value
  "Pull the underlying scalar(s) out of a Metabase parameter value record.
   Returns the value unchanged when `v` isn't a record we recognize."
  [v]
  (cond
    (unsupported-record? v)              :unsupported
    ;; Date — a single date string (`:s`).
    (instance? metabase.driver.common.parameters.Date v)         (:s v)
    ;; DateRange / DateTimeRange — render as ISO "start/end".
    (instance? metabase.driver.common.parameters.DateRange v)    (str (:start v) "/" (:end v))
    (instance? metabase.driver.common.parameters.DateTimeRange v) (str (:start v) "/" (:end v))
    ;; TemporalUnit wraps a raw scalar in `:value`.
    (params/TemporalUnit? v)             (:value v)
    ;; Plain map shape `{:type … :value …}` occasionally surfaces.
    (and (map? v) (contains? v :value))  (:value v)
    :else                                v))

(declare ->sparql-term)

(defn- ->sparql-term
  "Render a single parameter value as a SPARQL term. Returns nil when the value
   is `no-value` / nil / unsupported — callers should treat nil as 'leave the
   placeholder in place'."
  [v]
  (let [v (record-value v)]
    (cond
      (nil? v)              nil
      (= params/no-value v) nil
      (= :unsupported v)    (do (log/warnf "[sparql.params] Unsupported parameter type; placeholder left untouched")
                                nil)
      (sequential? v)       (let [terms (keep ->sparql-term v)]
                              (when (seq terms)
                                (str/join ", " terms)))
      (boolean? v)          (str v)
      (number? v)           (str v)
      (iri-shaped? v)       (str "<" v ">")
      (string? v)           (str "\"" (escape-sparql-string v) "\"")
      :else                 (do (log/warnf "[sparql.params] Unrecognized parameter value class %s; falling back to (str v)"
                                           (class v))
                                (str "\"" (escape-sparql-string (str v)) "\"")))))

(defn- substitute-one
  "Replace every `{{tag}}` placeholder for `tag-name` in `query` with `term`.
   Allows arbitrary whitespace inside the braces. Defends against regex
   meta-chars in `tag-name` and against `$` / `\\` in `term`."
  [query tag-name term]
  (let [pat (re-pattern (str "\\{\\{\\s*" (Pattern/quote tag-name) "\\s*\\}\\}"))]
    (str/replace query pat (Matcher/quoteReplacement term))))

(defn substitute-native-parameters
  "Substitute `{{tag}}` placeholders in `inner-query`'s `:query` string using
   the parameters / template-tags in `inner-query`. Returns the updated
   inner-query map.

   Tags whose value is missing (`no-value` / nil) leave their placeholder
   untouched and emit a warning; the SPARQL endpoint will then surface a clear
   parse error rather than silently dropping the constraint."
  [_driver inner-query]
  #_{:clj-kondo/ignore [:unresolved-var]}
  (let [query        (:query inner-query)
        param->value (params.values/query->params-map inner-query)
        substituted
        (reduce
         (fn [q [k v]]
           (let [tag-name (name k)]
             (if-let [term (->sparql-term v)]
               (substitute-one q tag-name term)
               (do (log/warnf "[sparql.params] Parameter %s has no value; leaving {{%s}} in query"
                              tag-name tag-name)
                   q))))
         query
         param->value)]
    (when-let [remaining (re-seq #"\{\{\s*([^{}\s][^{}]*?)\s*\}\}" substituted)]
      (log/warnf "[sparql.params] Unresolved placeholders remain after substitution: %s"
                 (vec (distinct (map second remaining)))))
    (log/debugf "[sparql.params] Substituted query: %s" substituted)
    (assoc inner-query :query substituted)))
