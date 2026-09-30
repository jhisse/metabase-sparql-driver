(ns metabase.driver.sparql.parameters
  "SPARQL Parameter Substitution for Metabase SPARQL Driver.

   Replaces `{{tag}}` placeholders in native SPARQL queries with their parameter
   values, rendering each value as a syntactically correct SPARQL term:

     - strings → `\"escaped\"`
     - IRIs (`http(s)://` or `urn:` values) → `<value>`
     - numbers / booleans → bare literal
     - dates → `\"2024-01-15\"^^<…XMLSchema#date>` (`#dateTime` when a time
       is given)
     - date ranges → the string literal `\"start/end\"`
     - sequential collections → comma-separated SPARQL terms (only valid inside
       `IN(...)` / `VALUES`; template authors must wrap accordingly)
     - `[[ … ]]` clauses → dropped when one of their parameters has no value;
       anywhere else a `{{…}}` without a value stays as written, which keeps a
       commented-out tag (`# … {{x}}`) intact
     - a text or date tag written right inside quotes (`'{{x}}'`) → an error,
       since its term would close them"
  (:require
   [clojure.string :as str]
   [metabase.driver-api.core :as driver-api]
   [metabase.driver.common.parameters :as params]
   ^{:clj-kondo/ignore [:deprecated-namespace]} [metabase.driver.common.parameters.parse :as params.parse]
   [metabase.driver.common.parameters.values :as params.values]
   [metabase.driver.sparql.uri :as uri]
   [metabase.util.log :as log]))

(defn- unsupported-kind
  "Return the user-facing name of a parameter value record we cannot render in
   SPARQL, or nil: Field Filters (SQL-shaped BETWEEN/IN clauses), referenced
   cards, snippets, and referenced tables."
  [v]
  (cond
    (params/FieldFilter? v)            "Field Filter"
    (params/ReferencedCardQuery? v)    "saved question"
    (params/ReferencedQuerySnippet? v) "snippet"
    (params/ReferencedTableQuery? v)   "table"))

(defn- record-value
  "Return the underlying scalar(s) of a Metabase parameter value record, a
   date range as the string `\"start/end\"`, or `v` unchanged when it isn't a
   record we recognize. Throws `unsupported-feature` for the kinds
   [[unsupported-kind]] names."
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
  "Render a Date parameter's `s` as a typed literal: a plain string never compares
   equal to, or orders against, an xsd:date value. xsd:dateTime needs seconds,
   which a `…THH:mm` value (with or without a timezone) lacks."
  [s]
  (str (uri/string-literal (str/replace s #"(T\d{2}:\d{2})(?=Z|[+-]\d|$)" "$1:00"))
       "^^<http://www.w3.org/2001/XMLSchema#" (if (str/includes? s "T") "dateTime" "date") ">"))

(declare ->sparql-term)

(defn- ->sparql-term
  "Render a single parameter value as a SPARQL term, or a sequential value as
   comma-separated terms. Returns nil when the value is `no-value`, nil, or an
   empty collection, which callers treat as a missing value."
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

(defn- quoted?
  "True when a tag is written right inside quotes: the query text `before` it
   ends with a quote, or the text `after` it starts with one. A tag on a `#`
   comment line never is, since its value stays in the comment."
  [before after]
  ;; Only a quote touching the tag counts: a tag further inside a string
  ;; (`"Hello {{x}}"`, or `'[[{{x}}]]'` across a clause edge) is not caught.
  ;; Catching it needs a SPARQL lexer over the query (closed #75), weighed
  ;; against a slip that already returns wrong rows for any normal value.
  (let [before  (if (string? before) before "")
        after   (if (string? after) after "")
        line    (subs before (inc (or (str/last-index-of before "\n") -1)))
        quotes  ["\"" "'"]]
    (and (not (str/starts-with? (str/triml line) "#"))
         (boolean (or (some #(str/ends-with? before %) quotes)
                      (some #(str/starts-with? after %) quotes))))))

(defn- substitute
  "Render parsed query tokens (strings, `Param`s, `Optional`s) as
   `[fragments missing]`: the query fragments, and the names of parameters
   without a value, which stay as written. An `[[ … ]]` clause is dropped whole
   when any of its parameters is missing. Throws when a tag whose term brings
   its own quotes (text, dates) is written right inside quotes (`'{{x}}'`):
   they would close the literal and let the value rewrite the query."
  [param->value tokens]
  (reduce
   (fn [[acc missing] [before token after]]
     (cond
       (string? token)         [(conj acc token) missing]
       (params/Param? token)   (if-let [term (->sparql-term (get param->value (:k token)))]
                                 (if (and (re-find #"[\"']" term) (quoted? before after))
                                   (throw (ex-info (format "The {{%s}} variable is written inside quotes. Remove them: the driver quotes text values itself."
                                                           (:k token))
                                                   {:type driver-api/qp.error-type.invalid-query}))
                                   [(conj acc term) missing])
                                 [(conj acc (str "{{" (:k token) "}}")) (conj missing (:k token))])
       (params/Optional? token) (let [[opt opt-missing] (substitute param->value (:args token))]
                                  [(cond-> acc (empty? opt-missing) (into opt)) missing])
       :else                   (throw (ex-info (str "The SPARQL driver cannot substitute " (pr-str token))
                                               {:type driver-api/qp.error-type.unsupported-feature}))))
   [[] []]
   (partition 3 1 (concat [nil] tokens [nil]))))

(defn substitute-native-parameters
  "Return `inner-query` with the `{{tag}}` placeholders in its `:query`
   replaced by their parameter values, and `[[ … ]]` clauses dropped when one
   of their parameters has no value. Any other tag without a value stays as
   written, with a logged warning. Throws when a tag with a value is written
   inside quotes."
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
