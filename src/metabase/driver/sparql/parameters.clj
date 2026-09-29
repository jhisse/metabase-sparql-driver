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
     - a value inside quotes → an error, unless it is a bare number or
       boolean: the quotes of a text or date term would close the literal
       around it"
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

(defn- part-text
  "Return the query text of a [[substitute]] part: a fragment, or a term."
  [part]
  (if (map? part) (::term part) part))

(defn- lex-states
  "Return an array with, at every index of `s` where a lexer step starts (and
   at the end of `s`), the lexer state there, and nil inside a step. States are
   `:code`, `:comment`, `:iri`, or the delimiter (`\"`, `'`, `\"\"\"`, `'''`)
   of the string literal being read. Follows SPARQL's tokens: a backslash
   escapes the next character (in strings, and in prefixed names such as
   `ex:it\\'s`), `#` starts a comment, and `<` opens an IRI only when a whole
   IRIREF follows."
  ^objects [^String s]
  (let [n      (count s)
        states (object-array (inc n))]
    (loop [i 0, state :code]
      (if (>= i n)
        (do (when (= i n) (aset states n state))
            states)
        (let [c (.charAt s i)]
          (aset states i state)
          (case state
            :comment (recur (inc i) (if (#{\newline \return} c) :code :comment))
            :iri     (recur (inc i) (if (= c \>) :code :iri))
            :code    (cond
                       (= c \\)                            (recur (+ i 2) :code)
                       (= c \#)                            (recur (inc i) :comment)
                       (#{\" \'} c)                        (let [delim (if (.regionMatches s i (str c c c) 0 3) (str c c c) (str c))]
                                                             (recur (+ i (count delim)) delim))
                       (and (= c \<) (uri/iriref-at? s i)) (recur (inc i) :iri)
                       :else                               (recur (inc i) :code))
            (cond
              (= c \\)                                           (recur (+ i 2) state)
              (.regionMatches s i ^String state 0 (count state)) (recur (+ i (count state)) :code)
              :else                                              (recur (inc i) state))))))))

(defn- unsafe-term
  "Return the first term in `parts` (query fragments, and terms as
   `{::term s ::k tag}`, joined as `text`) that the text around it could turn
   into query code, else nil. `text` is lexed once, and each term must start
   and end where a lexer step does, in the same state: in code, or in a
   comment. Inside a string literal only a bare number or boolean (or a list
   of them) is safe, since any other term brings the quotes that would close
   it. A text term inside `<...>` holds a `\"` or `<`, so the `<` before it
   is no IRI: the term stays one quoted literal and the endpoint rejects the
   broken IRI."
  [parts ^String text]
  ;; The query text is the question author's, who can write any SPARQL: this
  ;; catches an author's slip (a tag written inside quotes) that would let a
  ;; viewer's value rewrite the query, not a template built to fool the lexer
  ;; (endpoints differ on `\uXXXX` escapes and on `<` in a FILTER).
  (let [states (lex-states text)]
    (some (fn [[start part]]
            (when-let [term (::term part)]
              (let [state (aget states start)]
                (when-not (and state
                               (= state (aget states (+ start (count term))))
                               (or (#{:code :comment} state)
                                   (re-matches #"[\w.+-]+(?:, [\w.+-]+)*" term)))
                  part))))
          (map vector (reductions + 0 (map (comp count part-text) parts)) parts))))

(defn- substitute
  "Render parsed query tokens (strings, `Param`s, `Optional`s) as
   `[parts missing]`: the query fragments with each rendered term as
   `{::term s ::k tag}`, and the names of parameters without a value, which
   stay as written. An `[[ … ]]` clause is dropped whole when any of its
   parameters is missing."
  [param->value tokens]
  (reduce
   (fn [[acc missing] token]
     (cond
       (string? token)         [(conj acc token) missing]
       (params/Param? token)   (if-let [term (->sparql-term (get param->value (:k token)))]
                                 [(conj acc {::term term ::k (:k token)}) missing]
                                 [(conj acc (str "{{" (:k token) "}}")) (conj missing (:k token))])
       (params/Optional? token) (let [[opt opt-missing] (substitute param->value (:args token))]
                                  [(cond-> acc (empty? opt-missing) (into opt)) missing])
       :else                   (throw (ex-info (str "The SPARQL driver cannot substitute " (pr-str token))
                                               {:type driver-api/qp.error-type.unsupported-feature}))))
   [[] []]
   tokens))

(defn substitute-native-parameters
  "Return `inner-query` with the `{{tag}}` placeholders in its `:query`
   replaced by their parameter values, and `[[ … ]]` clauses dropped when one
   of their parameters has no value. Any other tag without a value stays as
   written, with a logged warning. Throws when a value could become query
   code: a value other than a bare number or boolean inside quotes, or one
   glued to a quote that would merge with its own (`\"\"{{x}}`), see
   [[unsafe-term]]."
  [_driver inner-query]
  #_{:clj-kondo/ignore [:unresolved-var]}
  (let [param->value      (params.values/query->params-map inner-query)
        ;; false: SPARQL comments are `#`, which the parser does not know;
        ;; a tag in one is left as written when it has no value.
        [parts missing]   (substitute param->value (params.parse/parse (:query inner-query) false))
        substituted       (str/join (map part-text parts))]
    (when-let [{::keys [k]} (when (some map? parts) (unsafe-term parts substituted))]
      (throw (ex-info (format "The {{%s}} variable is inside quotes, or glued to a quote next to it. Write it on its own, with spaces around it: the driver quotes text values and brackets IRIs itself."
                              k)
                      {:type driver-api/qp.error-type.invalid-query})))
    (when (seq missing)
      (log/warnf "[sparql.params] No value for %s; left as written" (vec (distinct missing))))
    (log/debugf "[sparql.params] Substituted query: %s" substituted)
    (assoc inner-query :query substituted)))
