(ns metabase.driver.sparql.mbql-test
  "Unit tests for the MBQL -> SPARQL transpiler.

   Pure helpers are tested directly. The stage-compilation functions reach the
   metadata provider through four private accessors
   (`field-id->metadata`, `table-id->class-uri`, `database-naming-context`,
   `database-default-language`); those are stubbed with `with-redefs-fn` so the
   full compile path can be exercised without a running Metabase app DB.
   Every compiled stage is also run through a real SPARQL parser, so fragment
   assertions cannot pass on a malformed query."
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [metabase.driver-api.core :as driver-api]
            [metabase.driver.sparql.mbql :as mbql]
            [metabase.driver.sparql.test-util :as tu]
            [metabase.driver.sparql.uri :as uri]))

;; ---------------------------------------------------------------------------
;; Pure helpers
;; ---------------------------------------------------------------------------

(deftest sanitize-var-name-test
  (let [f @#'mbql/sanitize-var-name]
    (is (= "naam" (f "naam")))
    (is (= "geboorte_plaats" (f "geboorte-plaats")))
    (is (= "a_b_c" (f "a.b/c")))
    (testing "a leading digit is escaped"
      (is (= "_1col" (f "1col"))))
    (testing "blank input yields a usable default"
      (is (= "v" (f ""))))))

(deftest field-token-accessors-test
  (let [id    @#'mbql/field-token->id
        opts  @#'mbql/field-token->opts
        alias @#'mbql/field-token->join-alias]
    (is (= 5 (id [:field 5 {:join-alias "J"}])))
    (is (= "naam" (id [:field "naam" nil])))
    (is (nil? (id [:not-a-field 5])))
    (is (= {:join-alias "J"} (opts [:field 5 {:join-alias "J"}])))
    (is (nil? (opts [:field 5 nil])))
    (is (= "J" (alias [:field 5 {:join-alias "J"}])))
    (is (nil? (alias [:field 5 nil])))))

(deftest literal->sparql-test
  (let [f @#'mbql/literal->sparql]
    (is (= "\"Alice\"" (f "Alice")))
    (is (= "25" (f 25)))
    (is (= "true" (f true)))
    (is (= "false" (f false)))
    (is (= "" (f nil)))
    (testing "embedded double quotes are escaped"
      (is (= "\"a\\\"b\"" (f "a\"b"))))
    (testing "backslashes are escaped (a trailing one no longer swallows the closing quote)"
      (is (= "\"foo\\\\\"" (f "foo\\")))
      (is (= "\"a\\\\b\"" (f "a\\b"))))
    (testing "newlines/tabs are escaped so the literal stays single-line"
      (is (= "\"a\\nb\"" (f "a\nb")))
      (is (= "\"a\\tb\"" (f "a\tb"))))))

(deftest condition->fk-ref-test
  (let [fk-ref @#'mbql/condition->fk-ref
        fk-id  @#'mbql/condition->fk-field-id]
    (testing "the non-join-alias side of an = is the FK ref"
      (is (= [:field 1 nil]
             (fk-ref [:= [:field 1 nil] [:field 2 {:join-alias "J"}]])))
      (is (= 1 (fk-id [:= [:field 1 nil] [:field 2 {:join-alias "J"}]]))))
    (testing "an :and wrapper is unwrapped to its first ="
      (is (= [:field 1 nil]
             (fk-ref [:and [:= [:field 1 nil] [:field 2 {:join-alias "J"}]]]))))))

(deftest collect-field-ids-test
  (let [f @#'mbql/collect-field-ids]
    (is (= #{1 2 3 4}
           (set (f {:fields   [[:field 1 nil] [:field 2 nil]]
                    :order-by [[:asc [:field 3 nil]]]
                    :filter   [:= [:field 4 nil] 5]}))))))

(deftest collect-joined-pairs-test
  (let [f @#'mbql/collect-joined-pairs]
    (is (= #{[2 "J" nil] [3 "J" 4]}
           (f {:fields [[:field 1 nil] [:field 2 {:join-alias "J"}] [:field 3 {:join-alias "J" :source-field 4}]]})))))

(deftest aggregation-helpers-test
  (let [unwrap  @#'mbql/unwrap-aggregation
        arg-tok @#'mbql/aggregation-arg-token]
    (is (= [:count] (unwrap [:aggregation-options [:count] {:name "c"}])))
    (is (= [:count] (unwrap [:count])))
    (is (= [:field 5 nil] (arg-tok [:sum [:field 5 nil]])))
    (is (nil? (arg-tok [:count])))))

(deftest aggregation->projection-test
  (let [f @#'mbql/aggregation->projection]
    (testing "arg-less count is a DISTINCT subject count in a base stage"
      (is (= {:select "(COUNT(DISTINCT ?subject) AS ?ag_0)" :var "ag_0"}
             (f [:count] 0 (constantly nil)))))
    (testing "arg-less count becomes COUNT(*) when count-all? is set"
      (is (= {:select "(COUNT(*) AS ?ag_0)" :var "ag_0"}
             (f [:count] 0 (constantly nil) true))))
    (testing "sum/avg/min/max/distinct projections"
      (is (= {:select "(SUM(?amount) AS ?ag_1)" :var "ag_1"}
             (f [:sum [:field "amount" nil]] 1 (constantly "amount"))))
      (is (= {:select "(MIN(?amount) AS ?ag_0)" :var "ag_0"}
             (f [:min [:field "amount" nil]] 0 (constantly "amount"))))
      (is (= {:select "(COUNT(DISTINCT ?amount) AS ?ag_0)" :var "ag_0"}
             (f [:distinct [:field "amount" nil]] 0 (constantly "amount")))))
    (testing "an :aggregation-options wrapper is transparent"
      (is (= {:select "(COUNT(*) AS ?ag_0)" :var "ag_0"}
             (f [:aggregation-options [:count] {:name "c"}] 0 (constantly nil) true))))
    (testing "an aggregation it cannot compile throws instead of dropping the column"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"does not support the stddev aggregation"
                            (f [:stddev [:field "amount" nil]] 0 (constantly "amount"))))
      (is (= driver-api/qp.error-type.unsupported-feature
             (try (f [:count-where [:> [:field "amount" nil] 1]] 0 (constantly "amount"))
                  (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))))))

(deftest compile-filter-expr-test
  (let [f #(@#'mbql/compile-filter-expr % {"naam" "naam" "leeftijd" "leeftijd"} {})]
    (is (= "(?naam = \"Jan\")"        (f [:= [:field "naam" nil] "Jan"])))
    (testing "a wrapped [:value ...] rhs is unwrapped"
      (is (= "(?naam = \"Jan\")"      (f [:= [:field "naam" nil] [:value "Jan" {}]]))))
    (is (= "(!BOUND(?naam))"          (f [:= [:field "naam" nil] nil])))
    (is (= "(BOUND(?naam))"           (f [:!= [:field "naam" nil] nil])))
    (is (= "(?leeftijd > 18)"         (f [:> [:field "leeftijd" nil] 18])))
    (is (= "(?naam != \"Jan\")"       (f [:!= [:field "naam" nil] "Jan"])))
    (testing "case-insensitive contains"
      (is (= "(CONTAINS(LCASE(STR(?naam)), LCASE(\"an\")))"
             (f [:contains [:field "naam" nil] "an" {:case-sensitive false}]))))
    (testing "boolean combinators"
      (is (= "((?naam = \"Jan\") && (?leeftijd > 18))"
             (f [:and [:= [:field "naam" nil] "Jan"] [:> [:field "leeftijd" nil] 18]])))
      (is (= "((?naam = \"Jan\") || (?naam = \"Piet\"))"
             (f [:or [:= [:field "naam" nil] "Jan"] [:= [:field "naam" nil] "Piet"]]))))
    (testing "boolean combinators keep every condition, not just the first two"
      (is (= "((?naam = \"Jan\") && (?leeftijd > 18) && (?naam != \"Piet\"))"
             (f [:and [:= [:field "naam" nil] "Jan"] [:> [:field "leeftijd" nil] 18] [:!= [:field "naam" nil] "Piet"]])))
      (is (= "((?naam = \"A\") || (?naam = \"B\") || (?naam = \"C\") || (?naam = \"D\"))"
             (f [:or [:= [:field "naam" nil] "A"] [:= [:field "naam" nil] "B"]
                 [:= [:field "naam" nil] "C"] [:= [:field "naam" nil] "D"]]))))
    (testing "a dangerous rhs (quote + backslash) is routed through the shared escaper, so the emitted SPARQL literal stays well-formed"
      (is (= (str "(?naam = " (uri/string-literal "a\"b\\") ")")
             (f [:= [:field "naam" nil] "a\"b\\"]))))
    (testing "a dangerous :contains needle is escaped inside STR() too"
      (is (= (str "(CONTAINS(STR(?naam), " (uri/string-literal "a\"b") "))")
             (f [:contains [:field "naam" nil] "a\"b"]))))
    (testing "IRI-valued fields render an IRI-shaped value as <IRI>, not a string literal"
      (with-redefs [mbql/field-id->metadata {5 {:name "country" :semantic-type :type/FK}
                                             6 {:name "homepage" :semantic-type :type/URL}
                                             7 {:name "subject" :database-type "uri"}
                                             2 {:name "name" :database-type "string"}}]
        (let [g #(@#'mbql/compile-filter-expr % {5 "country" 6 "homepage" 7 "subject" 2 "name"} {})
              iri "https://example.org/countries/AC28-7090"]
          (is (= (str "(?country = <" iri ">)")
                 (g [:= [:field 5 nil] iri]))
              "FK field + URL value → IRI term")
          (is (= (str "(?country != <" iri ">)")
                 (g [:!= [:field 5 nil] iri]))
              ":!= routes through the same term rendering")
          (is (= (str "(?subject = <" iri ">)")
                 (g [:= [:field 7 nil] iri]))
              "the subject/PK column (database-type \"uri\") is IRI-valued too")
          (is (= (str "(?subject = " (uri/iri-ref "urn:isbn:0451450523") ")")
                 (g [:= [:field 7 nil] "urn:isbn:0451450523"]))
              "urn: values count as IRI-shaped")
          (is (= (str "(?homepage != " (uri/string-literal iri) ")")
                 (g [:!= [:field 6 nil] iri]))
              ":type/URL columns hold literal xsd:anyURI values — they stay literals")
          (is (= "(?country = \"AC-123\")"
                 (g [:= [:field 5 nil] "AC-123"]))
              "FK field + schemeless value stays a string literal")
          (is (= (str "(?country = " (uri/iri-ref "HTTPS://EX.ORG/X") ")")
                 (g [:= [:field 5 nil] "HTTPS://EX.ORG/X"]))
              "uppercase schemes count — RFC 3987 schemes are case-insensitive")
          (is (= (str "(?country = " (uri/iri-ref "did:example:123") ")")
                 (g [:= [:field 5 nil] "did:example:123"]))
              "non-http(s)/urn schemes count on a field known to hold IRIs")
          (is (= (str "(?country = " (uri/iri-ref "Ref: 123") ")")
                 (g [:= [:field 5 nil] "Ref: 123"]))
              "on an IRI-valued field any scheme-shaped value becomes an IRI term (garbage matches nothing either way)")
          (is (= (str "(?name = \"" iri "\")")
                 (g [:= [:field 2 nil] iri]))
              "plain string field + IRI-shaped value stays a string literal")
          (is (= (str "(?country = " (uri/iri-ref "https://x.example/> } UNION { ?s ?p ?o . #") ")")
                 (g [:= [:field 5 nil] "https://x.example/> } UNION { ?s ?p ?o . #"]))
              "a hostile value cannot close the IRIREF — forbidden chars are percent-encoded"))))))

(deftest emit-optional-triple-escapes-iri-test
  (let [emit @#'mbql/emit-optional-triple]
    (testing "the property IRI is routed through uri/iri-ref, so an illegal char is percent-encoded"
      (is (= (str "  OPTIONAL { ?subject " (uri/iri-ref "http://example.org/a b") " ?t . }")
             (emit "http://example.org/a b" "t")))
      ;; a normal property URI is unchanged (iri-ref is a no-op)
      (is (= "  OPTIONAL { ?s <http://example.org/name> ?t . }"
             (emit "s" "http://example.org/name" "t"))))))

(deftest lang-filter-line-escapes-tag-test
  (let [lang-line @#'mbql/lang-filter-line]
    (testing "an escapable char in the language tag is escaped, not leaked raw into the FILTER literal"
      (is (= (str "  FILTER(!BOUND(?naam) || LANG(?naam) = \""
                  (uri/escape-string "en\"") "\" || LANG(?naam) = \"\")")
             (lang-line "naam" "en\""))))
    (testing "a normal BCP-47 tag is unchanged"
      (is (= "  FILTER(!BOUND(?naam) || LANG(?naam) = \"pt-BR\" || LANG(?naam) = \"\")"
             (lang-line "naam" "pt-BR"))))))

(deftest between-filter-test
  (let [f #(@#'mbql/compile-filter-expr % {"leeftijd" "leeftijd"} {})]
    (testing ":between compiles to a numeric range"
      (is (= "(?leeftijd >= 18 && ?leeftijd <= 65)"
             (f [:between [:field "leeftijd" nil] 18 65])))
      (testing "a [:value ...] wrapped bound is unwrapped"
        (is (= "(?leeftijd >= 18 && ?leeftijd <= 65)"
               (f [:between [:field "leeftijd" nil] [:value 18 {}] [:value 65 {}]])))))))

(deftest string-match-and-negation-filters-test
  (let [f #(@#'mbql/compile-filter-expr % {"naam" "naam"} {})]
    (testing "case-sensitive string matches compare the STR() of the value"
      (is (= "(STRSTARTS(STR(?naam), \"Ja\"))" (f [:starts-with [:field "naam" nil] "Ja"])))
      (is (= "(STRENDS(STR(?naam), \"an\"))"   (f [:ends-with [:field "naam" nil] "an"])))
      (is (= "(CONTAINS(STR(?naam), \"a\"))"   (f [:contains [:field "naam" nil] "a"]))))
    (testing "case-insensitive variants lower-case both sides"
      (is (= "(STRSTARTS(LCASE(STR(?naam)), LCASE(\"ja\")))"
             (f [:starts-with [:field "naam" nil] "ja" {:case-sensitive false}])))
      (is (= "(STRENDS(LCASE(STR(?naam)), LCASE(\"AN\")))"
             (f [:ends-with [:field "naam" nil] "AN" {:case-sensitive false}]))))
    (testing ":not wraps its inner expression (does-not-contain arrives as [:not [:contains …]])"
      (is (= "(!(CONTAINS(STR(?naam), \"x\")))"
             (f [:not [:contains [:field "naam" nil] "x"]]))))
    (testing ":is-null / :not-null map to BOUND checks"
      (is (= "(!BOUND(?naam))" (f [:is-null [:field "naam" nil]])))
      (is (= "(BOUND(?naam))"  (f [:not-null [:field "naam" nil]]))))
    (testing "a hostile needle stays inside its string literal"
      (is (= "(STRSTARTS(STR(?naam), \"\\\") || true || (\\\"\"))"
             (f [:starts-with [:field "naam" nil] "\") || true || (\""]))))))

(deftest avg-max-count-field-projection-test
  (let [f @#'mbql/aggregation->projection]
    (is (= {:select "(AVG(?amount) AS ?ag_0)" :var "ag_0"}
           (f [:avg [:field "amount" nil]] 0 (constantly "amount"))))
    (is (= {:select "(MAX(?amount) AS ?ag_2)" :var "ag_2"}
           (f [:max [:field "amount" nil]] 2 (constantly "amount"))))
    (testing "count over a field counts that variable, not the subject"
      (is (= {:select "(COUNT(?amount) AS ?ag_0)" :var "ag_0"}
             (f [:count [:field "amount" nil]] 0 (constantly "amount")))))))

(deftest unsupported-filter-clause-test
  (let [f #(@#'mbql/compile-filter-expr % {"naam" "naam" "leeftijd" "leeftijd"} {})]
    (testing "a custom-expression function on the lhs throws instead of dropping the filter"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"integer"
                            (f [:> [:integer [:field "leeftijd" nil]] 5])))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"lower"
                            (f [:= [:lower [:field "naam" nil]] "x"]))))
    (testing "an unsupported operator throws"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"regex-match-first"
                            (f [:regex-match-first [:field "naam" nil] "x"]))))
    (testing "an unsupported branch inside :and throws instead of silently keeping only half the filter"
      (is (thrown? clojure.lang.ExceptionInfo
                   (f [:and [:> [:field "leeftijd" nil] 1] [:> [:integer [:field "leeftijd" nil]] 5]]))))
    (testing "a field ref that resolves to no SPARQL variable throws instead of dropping the filter"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"unresolved column"
                            (f [:= [:field "onbekend" nil] "x"])))
      (is (thrown? clojure.lang.ExceptionInfo
                   (f [:and [:> [:field "leeftijd" nil] 1] [:= [:field "onbekend" nil] "x"]]))))
    (testing "the error is typed as an unsupported feature"
      (is (= driver-api/qp.error-type.unsupported-feature
             (try (f [:> [:integer [:field "leeftijd" nil]] 5])
                  (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))))))

(deftest temporal-filter-values-test
  (with-redefs [mbql/field-id->metadata {1 {:name "geboren" :base-type :type/Date}
                                         2 {:name "gewijzigd" :base-type :type/DateTimeWithTZ}
                                         3 {:name "naam" :base-type :type/Text}}
                mbql/query-zone (constantly (java.time.ZoneId/of "UTC"))
                mbql/query-now  (constantly (java.time.ZonedDateTime/parse "2026-09-26T10:15:30Z"))]
    (let [f      #(@#'mbql/compile-filter-expr % {1 "geboren" 2 "gewijzigd" 3 "naam"} {})
          date   "^^<http://www.w3.org/2001/XMLSchema#date>"
          dtime  "^^<http://www.w3.org/2001/XMLSchema#dateTime>"
          ;; dateTime bounds compare each value against the form matching its TZ()
          dt-cmp (fn [v op tz local]
                   (str "((TZ(?" v ") != \"\" && ?" v " " op " \"" tz "\"" dtime ")"
                        " || (TZ(?" v ") = \"\" && ?" v " " op " \"" local "\"" dtime "))"))]
      (testing "relative dates are resolved to the start of the unit, plus n units"
        (is (= (str "(?geboren >= \"2026-08-27\"" date ")")
               (f [:>= [:field 1 {:temporal-unit :default}] [:relative-datetime -30 :day]])))
        (is (= (str "(?geboren < \"2026-09-01\"" date ")")
               (f [:< [:field 1 nil] [:relative-datetime 0 :month]])))
        (is (= (str "(" (dt-cmp "gewijzigd" ">=" "2026-09-26T10:00:00Z" "2026-09-26T10:00:00") ")")
               (f [:>= [:field 2 nil] [:relative-datetime 0 :hour]])))
        (testing "year and quarter truncate to the start of the period (\"this year\", \"last 30 years\")"
          ;; regression: :year used to be *extracted* (-> 1996) instead of truncated
          (is (= (str "(?geboren >= \"1996-01-01\"" date ")")
                 (f [:>= [:field 1 nil] [:relative-datetime -30 :year]])))
          (is (= (str "(?geboren < \"2026-10-01\"" date ")")
                 (f [:< [:field 1 nil] [:relative-datetime 1 :quarter]])))))
      (testing ":current resolves to now"
        (is (= (str "(" (dt-cmp "gewijzigd" "<=" "2026-09-26T10:15:30Z" "2026-09-26T10:15:30") ")")
               (f [:<= [:field 2 nil] [:relative-datetime :current]]))))
      (testing "absolute dates render with the column's datatype"
        (is (= (str "(?geboren >= \"2024-01-01\"" date " && ?geboren <= \"2024-01-31\"" date ")")
               (f [:between [:field 1 nil]
                   [:absolute-datetime (java.time.LocalDate/parse "2024-01-01") :default]
                   [:absolute-datetime (java.time.LocalDate/parse "2024-01-31") :default]])))
        (is (= (str "(" (dt-cmp "gewijzigd" "=" "2024-01-01T00:00:00Z" "2024-01-01T00:00:00") ")")
               (f [:= [:field 2 nil] [:absolute-datetime (java.time.LocalDate/parse "2024-01-01") :default]])))
        (is (= (str "(" (dt-cmp "gewijzigd" ">=" "2024-01-01T06:30:00Z" "2024-01-01T06:30:00") ")")
               (f [:>= [:field 2 nil]
                   [:absolute-datetime (java.time.OffsetDateTime/parse "2024-01-01T08:30:00+02:00") :default]]))))
      (testing "a source-query column ref (no field metadata) takes its type from the ref's options"
        (let [g #(@#'mbql/compile-filter-expr % {"geboren" "geboren" "gewijzigd" "gewijzigd"} {})]
          (is (= (str "(?geboren >= \"2026-08-27\"" date ")")
                 (g [:>= [:field "geboren" {:base-type :type/Date}] [:relative-datetime -30 :day]])))
          (is (= (str "(" (dt-cmp "gewijzigd" ">=" "2024-01-01T00:00:00Z" "2024-01-01T00:00:00") ")")
                 (g [:>= [:field "gewijzigd" {:base-type :type/DateTime}]
                     [:absolute-datetime (java.time.LocalDate/parse "2024-01-01") :default]])))))
      (testing "null checks ignore the grouping (e.g. drilling into the \"(empty)\" bar of a by-month chart)"
        (is (= "(!BOUND(?geboren))" (f [:is-null [:field 1 {:temporal-unit :month}]])))
        (is (= "(BOUND(?geboren))" (f [:not-null [:field 1 {:temporal-unit :month}]])))
        (is (= "(!BOUND(?geboren))" (f [:= [:field 1 {:temporal-unit :month}] nil]))))
      (testing "a filter on a date grouped by a unit Metabase could not turn into a range throws"
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"month-of-year"
                              (f [:!= [:field 1 {:temporal-unit :month-of-year}] 1]))))
      (testing "any other clause as the value throws instead of being quoted as text"
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"field"
                              (f [:= [:field 3 nil] [:field 1 nil]])))))))

(deftest order-by-test
  (let [ob     #(@#'mbql/compile-order-by % {"naam" "naam" "leeftijd" "leeftijd"} {})
        agg-ob #(@#'mbql/compile-agg-order-by % (fn [t] (@#'mbql/var-for-token t {"naam" "naam"} {})))]
    (is (= "ORDER BY ASC(?naam)" (ob [[:asc [:field "naam" nil]]])))
    (is (= "ORDER BY DESC(?naam) ASC(?leeftijd)"
           (ob [[:desc [:field "naam" nil]] [:asc [:field "leeftijd" nil]]])))
    (is (nil? (ob [])))
    (testing "aggregation order-by can reference an aggregation by index"
      (is (= "ORDER BY DESC(?ag_0)" (agg-ob [[:desc [:aggregation 0]]])))
      (is (= "ORDER BY ASC(?naam)"  (agg-ob [[:asc [:field "naam" nil]]]))))))

(deftest var-for-token-test
  (let [f @#'mbql/var-for-token]
    (is (= "naam" (f [:field "naam" nil] {"naam" "naam"} {})))
    (testing "a join-alias token resolves through pair->target-var"
      (is (= "jvar" (f [:field "x" {:join-alias "J"}] {} {["x" "J"] "jvar"}))))
    (testing "an expression token resolves to its sanitized name"
      (is (= "my_col" (f [:expression "my-col"] {} {}))))))

(deftest compile-expression-test
  (let [f (fn [clause]
            (let [expr (@#'mbql/compile-expression clause {"a" "a" "b" "b"} {})
                  q    (str "SELECT * WHERE { BIND(" expr " AS ?x) }")]
              (is (nil? (tu/sparql-syntax-error q)) q)
              expr))]
    (testing "arithmetic"
      (is (= "(?a + 1)" (f [:+ [:field "a" nil] 1])))
      (is (= "(?a - ?b)" (f [:- [:field "a" nil] [:field "b" nil]])))
      (is (= "(?a * 2)" (f [:* [:field "a" nil] 2]))))
    (testing "string functions coerce args with STR()"
      (is (= "LCASE(STR(?a))" (f [:lower [:field "a" nil]])))
      (is (= "STRLEN(STR(?a))" (f [:length [:field "a" nil]])))
      (is (= "CONCAT(STR(?a), STR(?b))" (f [:concat [:field "a" nil] [:field "b" nil]]))))
    (testing "trim compiles to a REPLACE"
      (is (= "REPLACE(STR(?a), \"^\\\\s+|\\\\s+$\", \"\")" (f [:trim [:field "a" nil]]))))
    (testing "regexextract compiles to a first-match REPLACE"
      (is (= (str "IF(REGEX(STR(?a), \"[-0-9.]+\", \"s\"), "
                  "REPLACE(STR(?a), \"^.*?([-0-9.]+).*$\", \"$1\", \"s\"), (1/0))")
             (f [:regex-match-first [:field "a" nil] "[-0-9.]+"]))))
    (testing "a regexextract pattern taken from a column fails clearly"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"regexextract needs a literal pattern"
                            (f [:regex-match-first [:field "a" nil] [:field "b" nil]]))))
    (testing "a missing value and a case without default are null, not \"\""
      (is (= "COALESCE(?a, (1/0))" (f [:coalesce [:field "a" nil] nil])))
      (is (= "IF((?a > 5), \"big\", (1/0))"
             (f [:case [[[:> [:field "a" nil] 5] "big"]]]))))
    (testing "substring is 1-based SUBSTR"
      (is (= "SUBSTR(STR(?a), 2, 3)" (f [:substring [:field "a" nil] 2 3])))
      (is (= "SUBSTR(STR(?a), 2)" (f [:substring [:field "a" nil] 2]))))
    (testing "replace escapes quotes, newlines and regex metacharacters in find, and \\ and $ in the replacement"
      (is (= "REPLACE(STR(?a), \"a\\\"\\\\.b\\n\", \"\\\\$1\\\\\\\\\")"
             (f [:replace [:field "a" nil] "a\".b\n" "$1\\"]))))
    (testing "casts use the full xsd IRI constructor"
      (is (= "<http://www.w3.org/2001/XMLSchema#double>(?a)" (f [:float [:field "a" nil]])))
      (is (= (str "<http://www.w3.org/2001/XMLSchema#integer>(ROUND("
                  "<http://www.w3.org/2001/XMLSchema#decimal>(?a)))")
             (f [:integer [:field "a" nil]]))))
    (testing "coalesce / case"
      (is (= "COALESCE(?a, \"x\")" (f [:coalesce [:field "a" nil] "x"])))
      (is (= "IF((?a > 5), \"big\", \"small\")"
             (f [:case [[[:> [:field "a" nil] 5] "big"]] {:default "small"}]))))
    (testing "case predicates compile like filters, and a bare boolean column is used as is"
      (is (= "IF(?a, 1, 0)" (f [:case [[[:field "a" nil] 1]] {:default 0}])))
      (is (= "IF((CONTAINS(LCASE(STR(?a)), LCASE(\"x\"))), 1, 0)"
             (f [:case [[[:contains [:field "a" nil] "x" {:case-sensitive false}] 1]] {:default 0}]))))
    (testing "an unsupported function throws a clear error"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unsupported expression function"
                            (f [:totally-bogus [:field "a" nil]])))
      (is (= driver-api/qp.error-type.unsupported-feature
             (try (f [:datetime-diff [:field "a" nil] [:field "b" nil] :year])
                  (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))))))

(deftest inner-var-for-ref-test
  (let [f @#'mbql/inner-var-for-ref]
    (testing "a source-query column (string id) resolves to its sanitized name"
      (is (= "ag_0" (f [:field "ag_0" nil])))
      (is (= "geboorte_plaats" (f [:field "geboorte-plaats" nil]))))))

;; ---------------------------------------------------------------------------
;; Stage compilation (metadata accessors stubbed)
;; ---------------------------------------------------------------------------

(def ^:private base "https://example.org/")

(def ^:private fixture-fields
  {1  {:name "subject"}
   2  {:name "naam" :database-type "string"}
   3  {:name "leeftijd" :database-type "string"}
   4  {:name "geboorteplaats" :database-type "string"}
   10 {:name "label" :database-type "string"}
   11 {:name "geboorte-datum" :database-type "string"}})

(defn- parsed
  "Assert that a compiled stage's SPARQL parses, then return the stage."
  [{:keys [sparql] :as compiled}]
  (is (nil? (tu/sparql-syntax-error sparql)) sparql)
  compiled)

(defn- compile-stage* [stage]
  (parsed (@#'mbql/compile-stage stage)))

(def ^:private reused-agg-alias-error
  "KNOWN BUG: an aggregation on a derived stage re-binds `?ag_N`, which the
  sub-SELECT already projects. SPARQL 1.1 §18.2.4.1 forbids an `AS` target that
  is already in scope; RDF4J (and Jena) reject the query, Oxigraph tolerates it.
  Tests asserting this message must flip to `(is (nil? …))` once it is fixed."
  "projection alias 'ag_0' was previously used")

(defn- compile-base-stage* [stage expected-cols]
  (parsed (@#'mbql/compile-base-stage stage expected-cols)))

(defn- compile-derived-stage* [stage expected-cols]
  (parsed (@#'mbql/compile-derived-stage stage expected-cols)))

(defmacro ^:private with-fixture
  "Run `body` with the four metadata accessors stubbed for the test fixture."
  [& body]
  `(with-redefs-fn
     {#'mbql/field-id->metadata        (fn [id#] (get fixture-fields id#))
      #'mbql/table-id->class-uri       (constantly (str base "Persoon"))
      #'mbql/database-naming-context   (constantly {:default-graph base :prefixes []})
      #'mbql/database-default-language (constantly "")}
     (fn [] ~@body)))

(deftest compile-base-stage-select-test
  (with-fixture
    (let [{:keys [sparql vars]}
          (compile-stage* {:source-table 100
                           :fields [[:field 1 nil] [:field 2 nil] [:field 3 nil]]})]
      (is (= ["subject" "naam" "leeftijd"] vars))
      (is (str/includes? sparql "SELECT ?subject ?naam ?leeftijd"))
      (is (str/includes? sparql (str "?subject a <" base "Persoon> .")))
      (is (str/includes? sparql (str "OPTIONAL { ?subject <" base "naam> ?naam . }"))))))

(deftest compile-base-stage-filter-test
  (with-fixture
    (let [{:keys [sparql]}
          (compile-stage* {:source-table 100
                           :fields [[:field 1 nil] [:field 2 nil]]
                           :filter [:= [:field 2 nil] "Jan"]})]
      (is (str/includes? sparql "FILTER (?naam = \"Jan\")")))))

(deftest compile-base-stage-order-limit-test
  (with-fixture
    (let [{:keys [sparql]}
          (compile-stage* {:source-table 100
                           :fields [[:field 1 nil] [:field 2 nil]]
                           :order-by [[:asc [:field 2 nil]]]
                           :limit 10})]
      (is (str/includes? sparql "ORDER BY ASC(?naam)"))
      (is (str/includes? sparql "LIMIT 10")))))

(deftest compile-base-stage-aggregation-test
  (with-fixture
    (testing "count with a breakout produces a DISTINCT subject count + GROUP BY"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :aggregation [[:count]]
                             :breakout [[:field 2 nil]]})]
        (is (= ["naam" "ag_0"] vars))
        (is (str/includes? sparql "(COUNT(DISTINCT ?subject) AS ?ag_0)"))
        (is (str/includes? sparql "GROUP BY ?naam"))))
    (testing "sum aggregates the requested field"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :aggregation [[:sum [:field 3 nil]]]
                             :breakout [[:field 2 nil]]})]
        (is (= ["naam" "ag_0"] vars))
        (is (str/includes? sparql "(SUM(?leeftijd) AS ?ag_0)"))))
    (testing "a breakout without aggregations still groups (distinct values)"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :breakout [[:field 2 nil]]})]
        (is (= ["naam"] vars))
        (is (str/includes? sparql "SELECT ?naam\n"))
        (is (str/includes? sparql "GROUP BY ?naam"))))))

(deftest compile-base-stage-temporal-breakout-test
  (with-fixture
    (testing "a breakout with a temporal unit groups and orders by its bucket"
      (let [tok [:field 11 {:temporal-unit :month}]
            {:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :aggregation  [[:count]]
                             :breakout     [tok]
                             :order-by     [[:asc tok]]})]
        (is (= ["geboorte_datum_month" "ag_0"] vars))
        (is (str/includes? sparql "BIND(STRDT(CONCAT(SUBSTR(STR(?geboorte_datum),1,7),\"-01\")"))
        (is (str/includes? sparql "GROUP BY ?geboorte_datum_month"))
        (is (str/includes? sparql "ORDER BY ASC(?geboorte_datum_month)"))))
    (testing "every supported unit compiles to valid SPARQL"
      (doseq [unit (keys @#'mbql/temporal-bucket-exprs)]
        (testing unit
          (compile-stage* {:source-table 100
                           :aggregation  [[:count]]
                           :breakout     [[:field 11 {:temporal-unit unit}]]}))))
    (testing "a unit SPARQL cannot compute is rejected instead of grouping raw values"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"cannot group dates by week"
                            (compile-stage* {:source-table 100
                                             :aggregation  [[:count]]
                                             :breakout     [[:field 11 {:temporal-unit :week}]]}))))))

(deftest compile-base-stage-fk-join-test
  (with-fixture
    (testing "an implicit FK join repeats the FK path in the joined column's OPTIONAL"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil]
                                      [:field 2 nil]
                                      [:field 10 {:join-alias "Plaats"}]]
                             :joins [{:alias "Plaats" :fk-field-id 4}]})]
        (is (= ["subject" "naam" "Plaats__label"] vars))
        (is (str/includes? sparql
                           (str "OPTIONAL { ?subject <" base "geboorteplaats> ?Plaats_subject . }")))
        (is (str/includes? sparql
                           (str "OPTIONAL { ?subject <" base "geboorteplaats> ?Plaats_subject . ?Plaats_subject <" base "label> ?Plaats__label . }")))))))

(deftest compile-base-stage-field-also-joined-test
  (testing "a field that is also the display value of a self-referencing FK keeps its own variable"
    (with-fixture
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil]
                                      [:field 10 nil]
                                      [:field 10 {:source-field 4 :join-alias "Kent"}]]
                             :joins  [{:alias "Kent" :fk-field-id 4}]
                             :filter [:= [:field 10 nil] "Jan"]})]
        (is (= ["subject" "label" "Kent__label"] vars))
        (is (str/includes? sparql (str "OPTIONAL { ?subject <" base "label> ?label . }")))
        (is (str/includes? sparql "FILTER (?label = \"Jan\")"))))))

(deftest compile-base-stage-remap-inside-explicit-join-test
  (testing "the display value of a FK column inside an explicit join follows that FK"
    (let [fields {1  {:name "subject" :table-id 100}
                  4  {:name "werkgever" :table-id 100}
                  10 {:name "label" :table-id 200}
                  21 {:name "stad" :table-id 200 :semantic-type :type/FK}
                  22 {:name "zetel" :table-id 200 :semantic-type :type/FK}
                  23 {:name "notitie" :table-id 200}
                  30 {:name "label" :table-id 300}
                  31 {:name "subject" :table-id 300}}]
      (with-redefs-fn
        {#'mbql/field-id->metadata        (fn [id] (get fields id))
         #'mbql/table-id->class-uri       (constantly (str base "Persoon"))
         #'mbql/database-naming-context   (constantly {:default-graph base :prefixes []})
         #'mbql/database-default-language (constantly "")}
        (fn []
          (let [{:keys [sparql vars]}
                (compile-base-stage* {:source-table 100
                                      :fields [[:field 1 nil]
                                               [:field 10 {:join-alias "C"}]
                                               [:field 30 {:join-alias "C" :source-field 21}]
                                               [:field 30 {:join-alias "C" :source-field 22}]
                                               [:field 31 {:join-alias "C" :source-field 21}]]
                                      :joins  [{:alias     "C"
                                                :condition [:= [:field 4 nil] [:field 1 {:join-alias "C"}]]}]}
                                     [{:id 1} {:id 10 :lib/join-alias "C"}
                                      {:id 30 :lib/join-alias "C"} {:id 30 :lib/join-alias "C"}
                                      {:id 31 :lib/join-alias "C"}])]
            (testing "two FKs to the same field keep one variable each"
              (is (= ["subject" "C__label" "C__stad__label" "C__zetel__label" "C__stad_subject"] vars)))
            (testing "the hopped entity's subject is the hop variable"
              (is (str/includes? sparql (str "OPTIONAL { ?subject <" base "werkgever> ?C_subject . "
                                             "?C_subject <" base "stad> ?C__stad_subject . }"))))
            (is (str/includes? sparql (str "?C_subject <" base "stad> ?C__stad_subject . "
                                           "?C__stad_subject <" base "label> ?C__stad__label ."))))
          (testing "a column reached through a field that is not a FK fails clearly"
            (is (thrown-with-msg? clojure.lang.ExceptionInfo #"cannot follow werkgever: it is not a foreign key"
                                  (compile-stage* {:source-table 100
                                                   :fields [[:field 1 nil] [:field 10 {:source-field 4}]]})))))))))

(deftest compile-base-stage-explicit-self-join-test
  (testing "a self-join on a FK: the joined label and that FK's display value inside the join stay apart"
    (let [fields {1  {:name "subject" :table-id 100}
                  4  {:name "kent" :table-id 100 :semantic-type :type/FK}
                  10 {:name "label" :table-id 100}}]
      (with-redefs-fn
        {#'mbql/field-id->metadata        (fn [id] (get fields id))
         #'mbql/table-id->class-uri       (constantly (str base "Persoon"))
         #'mbql/database-naming-context   (constantly {:default-graph base :prefixes []})
         #'mbql/database-default-language (constantly "")}
        (fn []
          (let [{:keys [sparql vars]}
                (compile-base-stage* {:source-table 100
                                      :fields [[:field 1 nil]
                                               [:field 10 {:join-alias "P"}]
                                               [:field 10 {:join-alias "P" :source-field 4}]]
                                      :joins  [{:alias     "P"
                                                :condition [:= [:field 4 nil] [:field 1 {:join-alias "P"}]]}]}
                                     [{:id 1} {:id 10 :lib/join-alias "P"} {:id 10 :lib/join-alias "P"}])]
            (is (= ["subject" "P__label" "P__kent__label"] vars))
            (is (str/includes? sparql (str "?P_subject <" base "kent> ?P__kent_subject . "
                                           "?P__kent_subject <" base "label> ?P__kent__label .")))))))))

(deftest compile-base-stage-implicit-join-projection-test
  (testing "Lib's result-metadata strips :lib/join-alias from implicit-joinable
            columns (only `:fk-field-id` remains). The compiler must still project
            the qualified `?<Alias>__<prop>` var, not the unqualified `?prop` —
            otherwise FK display values come back empty in the UI."
    (let [persoon-tid 100
          geslacht-tid 200
          fields {1  {:name "subject"      :table-id persoon-tid}
                  20 {:name "geslacht"     :table-id persoon-tid}
                  40 {:name "waarde"       :table-id geslacht-tid}}]
      (with-redefs-fn
        {#'mbql/field-id->metadata        (fn [id] (get fields id))
         #'mbql/table-id->class-uri       (constantly (str base "Persoon"))
         #'mbql/database-naming-context   (constantly {:default-graph base :prefixes []})
         #'mbql/database-default-language (constantly "")}
        (fn []
          (let [stage {:source-table persoon-tid
                       :fields       [[:field 1 nil]
                                      [:field 20 nil]
                                      [:field 40 {:join-alias "Geslacht__via__geslacht"}]]
                       :joins        [{:alias        "Geslacht__via__geslacht"
                                       :source-table geslacht-tid
                                       :fk-field-id  20}]}
                ;; Lib's expected-cols for the remap column: `:fk-field-id` only,
                ;; no `:lib/join-alias`.
                expected [{:id 1}
                          {:id 20}
                          {:id 40 :fk-field-id 20}]
                {:keys [sparql vars]} (compile-base-stage* stage expected)]
            (testing "the remap column resolves to the qualified join target var"
              (is (= ["subject" "geslacht" "Geslacht__via__geslacht__waarde"] vars))
              (is (str/includes? sparql "?Geslacht__via__geslacht__waarde")))
            (testing "no bogus direct triple is emitted for the joined waarde fid"
              (is (not (str/includes? sparql
                                      (str "OPTIONAL { ?subject <" base "waarde> ?waarde . }")))))))))))

(deftest compile-base-stage-explicit-chained-join-test
  (testing "Two-hop EXPLICIT joins from the notebook editor (Item → Provider → Owner).
            The second join's :condition has :join-alias on BOTH sides, so the
            FK side is identified by 'alias ≠ this join's alias'."
    (let [item-tid     100
          provider-tid 200
          owner-tid    300
          fields {1  {:name "subject" :table-id item-tid}
                  20 {:name "provider" :table-id item-tid}
                  21 {:name "subject" :table-id provider-tid}
                  30 {:name "owner" :table-id provider-tid}
                  31 {:name "subject" :table-id owner-tid}
                  40 {:name "owner_name" :table-id owner-tid}}]
      (with-redefs-fn
        {#'mbql/field-id->metadata        (fn [id] (get fields id))
         #'mbql/table-id->class-uri       (constantly (str base "Item"))
         #'mbql/database-naming-context   (constantly {:default-graph base :prefixes []})
         #'mbql/database-default-language (constantly "")}
        (fn []
          (let [{:keys [sparql]}
                (compile-stage*
                 {:source-table item-tid
                  :aggregation  [[:distinct [:field 1 nil]]]
                  :breakout     [[:field 40 {:join-alias "Owner"}]]
                  :joins        [{:alias        "Provider"
                                  :source-table provider-tid
                                  :condition    [:= [:field 20 nil]
                                                 [:field 21 {:join-alias "Provider"}]]}
                                 {:alias        "Owner"
                                  :source-table owner-tid
                                  :condition    [:= [:field 30 {:join-alias "Provider"}]
                                                 [:field 31 {:join-alias "Owner"}]]}]})]
            (testing "Item → Provider hop"
              (is (str/includes? sparql
                                 (str "OPTIONAL { ?subject <" base "provider> ?Provider_subject . }"))))
            (testing "Provider → Owner hop (the bug fix — explicit-join chained case)"
              (is (str/includes? sparql
                                 (str "OPTIONAL { ?subject <" base "provider> ?Provider_subject . ?Provider_subject <" base "owner> ?Owner_subject . }"))))
            (testing "leaf property triple"
              (is (str/includes? sparql
                                 (str "OPTIONAL { ?subject <" base "provider> ?Provider_subject . ?Provider_subject <" base "owner> ?Owner_subject . ?Owner_subject <" base "owner_name> ?Owner__owner_name . }"))))))))))

(deftest compile-base-stage-explicit-chained-join-without-table-id-test
  (testing "Same chained explicit join, but `field-id->metadata` returns NO `:table-id`
            (mirrors a real Metabase metadata-provider result). The FK source alias
            must still be recovered from the condition's `:join-alias` on the FK token,
            not from the metadata's table-id."
    (let [item-tid     100
          provider-tid 200
          owner-tid    300
          ;; No :table-id on any of these — only :name.
          fields {1  {:name "subject"}
                  20 {:name "provider"}
                  21 {:name "subject"}
                  30 {:name "owner"}
                  31 {:name "subject"}
                  40 {:name "owner_name"}}]
      (with-redefs-fn
        {#'mbql/field-id->metadata        (fn [id] (get fields id))
         #'mbql/table-id->class-uri       (constantly (str base "Item"))
         #'mbql/database-naming-context   (constantly {:default-graph base :prefixes []})
         #'mbql/database-default-language (constantly "")}
        (fn []
          (let [{:keys [sparql]}
                (compile-stage*
                 {:source-table item-tid
                  :aggregation  [[:distinct [:field 1 nil]]]
                  :breakout     [[:field 40 {:join-alias "Owner"}]]
                  :joins        [{:alias        "Provider"
                                  :source-table provider-tid
                                  :condition    [:= [:field 20 nil]
                                                 [:field 21 {:join-alias "Provider"}]]}
                                 {:alias        "Owner"
                                  :source-table owner-tid
                                  :condition    [:= [:field 30 {:join-alias "Provider"}]
                                                 [:field 31 {:join-alias "Owner"}]]}]})]
            (is (str/includes? sparql
                               (str "OPTIONAL { ?subject <" base "provider> ?Provider_subject . ?Provider_subject <" base "owner> ?Owner_subject . }"))
                "Owner FK triple must be anchored on ?Provider_subject even without :table-id metadata")))))))

(deftest compile-base-stage-chained-fk-join-test
  (testing "a 2-hop implicit join chain (Item → Provider → Owner) anchors the second
            join's FK triple to the first join's intermediate var, not ?subject"
    (let [fields {1  {:name "subject"}
                  20 {:name "provider"   :table-id 100}
                  30 {:name "owner"      :table-id 200}
                  40 {:name "owner_name" :table-id 300}}]
      (with-redefs-fn
        {#'mbql/field-id->metadata        (fn [id] (get fields id))
         #'mbql/table-id->class-uri       (constantly (str base "Item"))
         #'mbql/database-naming-context   (constantly {:default-graph base :prefixes []})
         #'mbql/database-default-language (constantly "")}
        (fn []
          (let [{:keys [sparql]}
                (compile-stage*
                 {:source-table 100
                  :aggregation  [[:count]]
                  :breakout     [[:field 40 {:join-alias "Owner"}]]
                  :joins        [{:alias "Provider" :source-table 200 :fk-field-id 20}
                                 {:alias "Owner"    :source-table 300 :fk-field-id 30}]})]
            (testing "Item → Provider hop is anchored on ?subject"
              (is (str/includes? sparql
                                 (str "OPTIONAL { ?subject <" base "provider> ?Provider_subject . }"))))
            (testing "Provider → Owner hop is anchored on ?Provider_subject (the bug fix)"
              (is (str/includes? sparql
                                 (str "OPTIONAL { ?subject <" base "provider> ?Provider_subject . ?Provider_subject <" base "owner> ?Owner_subject . }"))))
            (testing "leaf property triple anchors on ?Owner_subject"
              (is (str/includes? sparql
                                 (str "OPTIONAL { ?subject <" base "provider> ?Provider_subject . ?Provider_subject <" base "owner> ?Owner_subject . ?Owner_subject <" base "owner_name> ?Owner__owner_name . }"))))))))))

(deftest compile-derived-stage-aggregation-test
  (with-fixture
    (testing "an aggregation layered on a saved card compiles to a single column"
      ;; This is the regression case: a 'Minimum of Count' on a count-by-breakout card.
      (let [card {:source-table 100 :aggregation [[:count]] :breakout [[:field 2 nil]]}
            {:keys [sparql vars]}
            (@#'mbql/compile-stage {:source-query card
                                    :aggregation [[:min [:field "ag_0" nil]]]})]
        (is (= reused-agg-alias-error (tu/sparql-syntax-error sparql)))
        (is (= ["ag_0"] vars))
        (is (str/includes? sparql "(MIN(?ag_0) AS ?ag_0)"))
        (testing "the outer stage adds no GROUP BY (only the inner card's remains)"
          (is (= 1 (count (re-seq #"GROUP BY" sparql)))))))))

(deftest compile-derived-stage-passthrough-test
  (with-fixture
    (testing "with no outer clauses the inner card columns pass straight through"
      (let [card {:source-table 100 :aggregation [[:count]] :breakout [[:field 2 nil]]}
            {:keys [vars]} (compile-stage* {:source-query card})]
        (is (= ["naam" "ag_0"] vars))))))

(deftest compile-derived-stage-remap-test
  (with-fixture
    (testing "an outer-stage remap reads the label through a fresh var compared with ="
      ;; A plain OPTIONAL off ?geboorteplaats would bind it, when a row has no FK,
      ;; to every node carrying a label.
      (let [{:keys [sparql]}
            (compile-stage* {:source-query {:source-table 100
                                            :fields       [[:field 1 nil] [:field 4 nil]]}
                             :fields       [[:field "geboorteplaats" nil]
                                            [:field 10 {:join-alias "Plaats"}]]
                             :joins        [{:alias     "Plaats"
                                             :condition [:= [:field "geboorteplaats" nil]
                                                         [:field 1 {:join-alias "Plaats"}]]}]})]
        (is (str/includes? sparql
                           (str "OPTIONAL { ?geboorteplaats_node <" base "label> ?Plaats__label . "
                                "FILTER(?geboorteplaats_node = ?geboorteplaats) }")))))))

(deftest compile-derived-stage-over-temporal-bucket-test
  (with-fixture
    (testing "an outer stage finds a bucketed column by its raw name"
      (let [card {:source-table 100
                  :aggregation  [[:count]]
                  :breakout     [[:field 11 {:temporal-unit :month}]]}
            {:keys [sparql vars]}
            (compile-stage* {:source-query card
                             :aggregation  [[:sum [:field "count" nil]]]
                             :breakout     [[:field "geboorte-datum" nil]]})]
        (is (= "geboorte_datum_month" (first vars)))
        (is (= 2 (count (re-seq #"GROUP BY \?geboorte_datum_month" sparql)))
            "the outer stage groups by the bucket too")))))

(deftest compile-derived-stage-outer-filter-test
  (with-fixture
    (testing "an outer filter on an aggregated card resolves a column whose name is not a valid SPARQL var"
      (let [card {:source-table 100 :aggregation [[:count]] :breakout [[:field 11 nil]]}
            {:keys [sparql]}
            (@#'mbql/compile-stage {:source-query card
                                    :aggregation  [[:count]]
                                    :filter [:= [:field "geboorte-datum" nil] "x"]})]
        (is (= reused-agg-alias-error (tu/sparql-syntax-error sparql)))
        (is (str/includes? sparql "FILTER (?geboorte_datum = \"x\")"))))
    (testing "an outer filter on a saved card is applied around the sub-SELECT"
      (let [card {:source-table 100 :aggregation [[:count]] :breakout [[:field 2 nil]]}
            {:keys [sparql vars]}
            (compile-stage* {:source-query card
                             :filter [:= [:field "naam" nil] "Jan"]})]
        (is (= ["naam" "ag_0"] vars))
        (is (str/includes? sparql "FILTER (?naam = \"Jan\")"))))))

(deftest compile-derived-stage-outer-count-test
  (with-fixture
    (testing "an arg-less count on a derived stage uses COUNT(*) (no ?subject available)"
      (let [card {:source-table 100 :aggregation [[:count]] :breakout [[:field 2 nil]]}
            {:keys [sparql vars]}
            (compile-stage* {:source-query card
                             :aggregation [[:count]]
                             :breakout [[:field "naam" nil]]})]
        (is (= ["naam" "ag_0"] vars))
        (is (str/includes? sparql "(COUNT(*) AS ?ag_0)"))
        (is (str/includes? sparql "GROUP BY ?naam"))))))

(deftest compile-derived-stage-filter-on-aggregation-test
  (with-fixture
    (testing "drilling on an aggregation value (filter references it by Lib's name) is applied"
      (let [card {:source-table 100 :aggregation [[:count]] :breakout [[:field 2 nil]]}
            {:keys [sparql vars]}
            (compile-stage* {:source-query card
                             :filter [:< [:field "count" nil] 12]})]
        (is (= ["naam" "ag_0"] vars))
        (is (str/includes? sparql "FILTER (?ag_0 < 12)")
            "the `count` column reference resolves to the ?ag_0 SPARQL variable")))
    (testing "a named aggregation resolves by its :aggregation-options name"
      (let [card {:source-table 100
                  :aggregation [[:aggregation-options [:sum [:field 3 nil]] {:name "total"}]]
                  :breakout    [[:field 2 nil]]}
            {:keys [sparql]}
            (compile-stage* {:source-query card
                             :filter [:>= [:field "total" nil] 100]})]
        (is (str/includes? sparql "FILTER (?ag_0 >= 100)"))))
    (testing "duplicate aggregation names are disambiguated count, count_2, …"
      (let [f @#'mbql/aggregation-name->var]
        (is (= {"count" "ag_0" "count_2" "ag_1"}
               (f [[:count] [:distinct [:field 3 nil]]])))))))

(deftest compile-derived-stage-filter-on-joined-breakout-test
  (with-fixture
    (testing "an outer filter on a joined breakout column resolves via Lib's expected-cols name"
      (let [card {:source-table 100
                  :aggregation [[:count]]
                  :breakout    [[:field 10 {:join-alias "Place"}]]
                  :joins       [{:alias "Place" :fk-field-id 4}]}
            ;; Lib names the joined breakout column "Birthplace" — different from the
            ;; driver's invented SPARQL var "Place__label".
            expected [{:name "Birthplace"} {:name "count"}]
            {:keys [sparql vars]}
            (compile-derived-stage*
             {:source-query card
              :filter [:= [:field "Birthplace" nil] "Leuven"]}
             expected)]
        (is (= ["Place__label" "ag_0"] vars))
        (is (str/includes? sparql "FILTER (?Place__label = \"Leuven\")")
            "the Lib column name resolves to the joined SPARQL variable")))
    (testing "columns match inner vars by name when an FK remap reorders the sub-SELECT"
      ;; The FK remap puts the joined label first in the sub-SELECT, while Lib
      ;; lists the FK, the count, then the label.
      (let [card {:source-table 100
                  :aggregation [[:count]]
                  :breakout    [[:field 10 {:join-alias "Place"}] [:field 4 nil]]
                  :joins       [{:alias "Place" :fk-field-id 4}]}
            expected [{:name "geboorteplaats"}
                      {:name "count"}
                      {:name "label" :fk-field-id 4 :lib/desired-column-alias "Place__label"}]
            {:keys [sparql vars]}
            (compile-derived-stage*
             {:source-query card
              :filter [:> [:field "count" nil] 1]}
             expected)]
        (is (= ["geboorteplaats" "ag_0" "Place__label"] vars))
        (is (str/includes? sparql "FILTER (?ag_0 > 1)"))))
    (testing "the same name-aliasing applies to order-by on a joined breakout column"
      (let [card {:source-table 100
                  :aggregation [[:count]]
                  :breakout    [[:field 10 {:join-alias "Place"}]]
                  :joins       [{:alias "Place" :fk-field-id 4}]}
            expected [{:name "Birthplace"} {:name "count"}]
            {:keys [sparql]}
            (compile-derived-stage*
             {:source-query card
              :order-by [[:asc [:field "Birthplace" nil]]]}
             expected)]
        (is (str/includes? sparql "ORDER BY ASC(?Place__label)"))))))

(deftest compile-base-stage-expression-test
  (with-fixture
    (testing "a custom column emits a BIND and is projected"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 2 nil] [:expression "upper_naam"]]
                             :expressions {"upper_naam" [:upper [:field 2 nil]]}})]
        (is (some #{"upper_naam"} vars))
        (is (str/includes? sparql "BIND(UCASE(STR(?naam)) AS ?upper_naam)"))
        (is (str/includes? sparql "SELECT ?subject ?naam ?upper_naam"))))
    (testing "a field referenced only inside an expression still gets its triple"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:expression "len"]]
                             :expressions {"len" [:length [:field 3 nil]]}})]
        (is (str/includes? sparql (str "OPTIONAL { ?subject <" base "leeftijd> ?leeftijd . }")))
        (is (str/includes? sparql "BIND(STRLEN(STR(?leeftijd)) AS ?len)"))))
    (testing "filter and order-by can reference an expression"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:expression "len"]]
                             :expressions {"len" [:length [:field 2 nil]]}
                             :filter [:> [:expression "len"] 3]
                             :order-by [[:desc [:expression "len"]]]})]
        (is (str/includes? sparql "FILTER (?len > 3)"))
        (is (str/includes? sparql "ORDER BY DESC(?len)"))))
    (testing "an expression is bound after the expressions it references"
      ;; a → b → … → i, so name order is the reverse of dependency order
      (let [names (map str "abcdefghi")
            exprs (into {"i" [:length [:field 2 nil]]}
                        (map (fn [n nxt] [n [:* [:expression nxt] 2]]) names (rest names)))
            {:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields (mapv #(vector :expression %) names)
                             :expressions exprs})
            bind-order (map second (re-seq #"BIND\(.* AS \?(\w+)\)" sparql))]
        (is (= (reverse names) bind-order))))
    (testing "a custom column whose variable another column already uses fails clearly"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Rename the custom column \"geboorte datum\""
                            (@#'mbql/compile-stage {:source-table 100
                                                    :fields [[:field 11 nil] [:expression "geboorte datum"]]
                                                    :expressions {"geboorte datum" [:upper [:field 11 nil]]}})))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"its SPARQL variable \?x_y is already used"
                            (@#'mbql/compile-stage {:source-table 100
                                                    :fields [[:expression "x-y"] [:expression "x y"]]
                                                    :expressions {"x-y" [:upper [:field 2 nil]]
                                                                  "x y" [:lower [:field 2 nil]]}})))
      (testing "including the variables of date buckets and aggregations"
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Rename the custom column \"geboorte_datum_month\""
                              (@#'mbql/compile-stage {:source-table 100
                                                      :breakout [[:field 11 {:temporal-unit :month}]]
                                                      :expressions {"geboorte_datum_month" [:upper [:field 2 nil]]}})))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Rename the custom column \"ag_0\""
                              (@#'mbql/compile-stage {:source-table 100
                                                      :aggregation [[:count]]
                                                      :expressions {"ag_0" [:upper [:field 2 nil]]}})))))))

;; ---------------------------------------------------------------------------
;; Lib-driven projection (the column-count-mismatch fix)
;; ---------------------------------------------------------------------------

(deftest reconcile-base-projection-test
  (with-fixture
    (let [f   @#'mbql/reconcile-base-projection
          ctx {:field-id->var           {2 "naam" 3 "leeftijd"}
               :pair->target-var        {[10 "Plaats"] "Plaats__label"}
               :alias->intermediate-var {"Plaats" "Plaats_subject"}
               :join-path               {"Plaats" [(str "?subject <" base "geboorteplaats> ?Plaats_subject .")]}
               :naming                  {:default-graph base :prefixes []}}]
      (testing "columns the compiler already projects reuse their variable"
        (is (= {:vars ["subject" "naam" "Plaats__label"] :triples []}
               (f [{:id 1} {:id 2} {:id 10 :lib/join-alias "Plaats"}] ctx))))
      (testing "a joined column the compiler missed is synthesized off the intermediate var"
        (is (= {:vars    ["subject" "Plaats__naam"]
                :triples [(str "  OPTIONAL { ?subject <" base "geboorteplaats> ?Plaats_subject . ?Plaats_subject <" base "naam> ?Plaats__naam . }")]}
               (f [{:id 1} {:id 2 :lib/join-alias "Plaats"}] ctx))))
      (testing "an unresolvable column still gets a (placeholder) variable"
        (is (= {:vars ["undefined_1"] :triples []}
               (f [{:lib/join-alias "Nope"}] ctx)))))))

(deftest compile-base-stage-lib-driven-projection-test
  (with-fixture
    (testing "the SELECT is reconciled against Lib's expected columns"
      (let [stage    {:source-table 100
                      :fields [[:field 1 nil] [:field 2 nil]
                               [:field 10 {:join-alias "Plaats"}]]
                      :joins  [{:alias "Plaats" :fk-field-id 4}]}
            ;; Lib expects an extra joined column (id 3) the :fields list omits —
            ;; the FK-remap-on-a-remap case behind the recurring column mismatch.
            expected [{:id 1} {:id 2}
                      {:id 10 :lib/join-alias "Plaats"}
                      {:id 3  :lib/join-alias "Plaats"}]
            {:keys [sparql vars]} (compile-base-stage* stage expected)]
        (is (= 4 (count vars)) "one SELECT variable per Lib expected column")
        (is (= ["subject" "naam" "Plaats__label" "Plaats__leeftijd"] vars))
        (is (str/includes? sparql "SELECT ?subject ?naam ?Plaats__label ?Plaats__leeftijd"))
        (testing "the missing column is synthesized off the join's intermediate var"
          (is (str/includes?
               sparql
               (str "OPTIONAL { ?subject <" base "geboorteplaats> ?Plaats_subject . ?Plaats_subject <" base "leeftijd> ?Plaats__leeftijd . }"))))))))

(deftest compile-base-stage-lib-driven-order-test
  (with-fixture
    (testing "expected-cols drives column order, independent of :fields order"
      (let [stage {:source-table 100
                   :fields [[:field 1 nil] [:field 2 nil] [:field 3 nil]]}
            {:keys [vars]} (compile-base-stage* stage [{:id 1} {:id 3} {:id 2}])]
        (is (= ["subject" "leeftijd" "naam"] vars))))))

(deftest compile-derived-stage-lib-driven-projection-test
  (with-fixture
    (testing "expected-cols drives the derived-stage SELECT and preserves the column count"
      (let [card {:source-table 100 :aggregation [[:count]] :breakout [[:field 2 nil]]}
            ;; the inner card projects [naam ag_0]; Lib expects a third column
            {:keys [vars]} (compile-derived-stage*
                            {:source-query card}
                            [{:id 2} {:id 3} {:id 99 :lib/join-alias "Missing"}])]
        (is (= 3 (count vars)))
        (is (= ["naam" "ag_0"] (take 2 vars)))
        (is (str/starts-with? (last vars) "undefined_"))))))

(deftest compile-base-stage-lang-filter-test
  (testing "rdf:langString columns get a LANG filter when a default language is set"
    (with-redefs-fn
      {#'mbql/field-id->metadata        (fn [id] (get {1 {:name "subject"}
                                                       2 {:name "naam" :database-type "langString"}
                                                       4 {:name "plaats"}}
                                                      id))
       #'mbql/table-id->class-uri       (constantly (str base "Persoon"))
       #'mbql/database-naming-context   (constantly {:default-graph base :prefixes []})
       #'mbql/database-default-language (constantly "nl")}
      (fn []
        (let [{:keys [sparql]}
              (compile-stage* {:source-table 100
                               :fields [[:field 1 nil] [:field 2 nil] [:field 2 {:join-alias "P"}]]
                               :joins  [{:alias "P" :fk-field-id 4}]})]
          (is (str/includes?
               sparql
               "FILTER(!BOUND(?naam) || LANG(?naam) = \"nl\" || LANG(?naam) = \"\")"))
          (testing "also on a joined column"
            (is (str/includes?
                 sparql
                 "FILTER(!BOUND(?P__naam) || LANG(?P__naam) = \"nl\" || LANG(?P__naam) = \"\")"))))))))
