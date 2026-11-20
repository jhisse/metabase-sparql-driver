(ns metabase.driver.sparql.mbql-native-test
  "Hermetic tests for the driver/mbql->native entry point as the QP calls it:
  real pMBQL built with Lib over the smoke-fixture provider, preprocessed and
  compiled (no endpoint, no app DB) and run through a SPARQL parser. Covers
  what mbql_test.clj stubs away — QP preprocessing, Lib's returned-columns
  reconciliation and the pMBQL -> legacy conversion — without the Docker
  harness e2e_test.clj needs."
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [metabase.driver.sparql.test-util :as tu]
            [metabase.lib.core :as lib]))

(defn- ->sparql
  "Compile `q` to SPARQL, asserting that the result parses."
  [q]
  (let [sparql (:query (tu/compile-query q))]
    (is (nil? (tu/sparql-syntax-error sparql)) sparql)
    sparql))

(defn- select-vars
  "Projected variables of the outermost SELECT line, in order: bare `?v` and
  the targets of `(… AS ?v)`."
  [sparql]
  (->> (re-find #"(?m)^SELECT (.*)$" sparql)
       second
       (re-seq #"(?:^|\s)\?(\w+)(?=\s|$)|AS \?(\w+)\)")
       (map #(or (nth % 1) (nth % 2)))))

(deftest default-projection-test
  (testing "a bare table query projects every Lib column, subject first"
    (let [sparql (->sparql (tu/person-query))]
      (is (= "subject" (first (select-vars sparql))))
      (is (= 7 (count (select-vars sparql))))
      (is (str/includes? sparql "?subject a <https://example.org/Person> .")))))

(deftest fields-projection-follows-lib-test
  (testing "with-fields narrows the SELECT to exactly Lib's returned columns"
    (let [q     (tu/person-query)
          label (tu/column q tu/rdfs-label)
          age   (tu/column q "age")]
      (is (= 2 (count (select-vars (->sparql (lib/with-fields q [label age])))))))))

(deftest filters-test
  (let [q     (tu/person-query)
        label (tu/column q tu/rdfs-label)
        age   (tu/column q "age")
        subj  (tu/column q "subject")
        knows (tu/column q "knows")]
    (testing "a quote/backslash in a user value stays inside the literal"
      (is (str/includes? (->sparql (lib/filter q (lib/= label "a\"b\\")))
                         "\"a\\\"b\\\\\"")))
    (testing "equality on the subject and on an IRI-valued column emits <iri> terms"
      (is (str/includes? (->sparql (lib/filter q (lib/= subj "https://example.org/alice")))
                         "<https://example.org/alice>"))
      (is (str/includes? (->sparql (lib/filter q (lib/= knows "https://example.org/bob")))
                         "<https://example.org/bob>")))
    (testing "between compiles to a closed numeric range"
      (is (str/includes? (->sparql (lib/filter q (lib/between age 26 35)))
                         ">= 26 && ?age <= 35")))
    (testing "an unsupported custom expression fails loudly instead of dropping the filter"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"does not support the length\(\) function"
                            (->sparql (lib/filter q (lib/> (lib/length label) 3))))))))

(deftest negated-filters-keep-unbound-rows-test
  (testing "is not and does not contain keep the rows without a value, as SQL does"
    (let [q     (tu/person-query)
          knows (tu/column q "knows")]
      (is (str/includes? (->sparql (lib/filter q (lib/!= knows "https://example.org/bob")))
                         "FILTER (!BOUND(?knows) || !(?knows = <https://example.org/bob>))"))
      (is (str/includes? (->sparql (lib/filter q (lib/does-not-contain knows "bob")))
                         "FILTER (!BOUND(?knows) || !(CONTAINS(STR(?knows), \"bob\")))")))))

(deftest order-by-limit-test
  (let [q      (tu/person-query)
        age    (tu/column q "age")
        sparql (->sparql (-> q (lib/order-by age :desc) (lib/limit 1)))]
    (is (str/includes? sparql "ORDER BY DESC(?age)"))
    (is (str/ends-with? (str/trim sparql) "LIMIT 1"))))

(deftest aggregation-test
  (testing "count is a DISTINCT subject count"
    (is (str/includes? (->sparql (lib/aggregate (tu/person-query) (lib/count)))
                       "(COUNT(DISTINCT ?subject) AS ?ag_0)")))
  (testing "count by breakout groups by the breakout var"
    (let [q      (tu/person-query)
          label  (tu/column q lib/breakoutable-columns tu/rdfs-label)
          sparql (->sparql (-> q (lib/breakout label) (lib/aggregate (lib/count))))]
      (is (re-find #"GROUP BY \?\w+" sparql))
      (is (= 2 (count (select-vars sparql)))))))

(deftest derived-stage-filter-on-aggregation-test
  (testing "an outer stage filters the inner aggregation by its Lib name"
    (let [base      (let [q (tu/person-query)]
                      (-> q
                          (lib/breakout (tu/column q lib/breakoutable-columns tu/rdfs-label))
                          (lib/aggregate (lib/count))
                          (lib/append-stage)))
          count-col (tu/column base lib/filterable-columns "count")
          sparql    (->sparql (lib/filter base (lib/> count-col 1)))]
      (is (str/includes? sparql "FILTER (?ag_0 > 1)")))))

(deftest nested-aggregation-test
  (let [q        (tu/person-query)
        counts   (-> q
                     (lib/breakout (tu/column q lib/breakoutable-columns tu/rdfs-label))
                     (lib/aggregate (lib/count))
                     (lib/append-stage))
        count-of (fn [stage] (first (filter #(= "count" (:name %)) (lib/aggregable-columns stage nil))))]
    (testing "an aggregation over an aggregation binds a variable the sub-SELECT does not project"
      (is (str/includes? (->sparql (lib/aggregate counts (lib/min (count-of counts))))
                         "SELECT (MIN(?ag_0) AS ?ag1_0)")))
    (testing "a count by count: Lib names the breakout `count` and the new count `count_2`"
      (let [count-of-counts (-> counts
                                (lib/breakout (tu/column counts lib/breakoutable-columns "count"))
                                (lib/aggregate (lib/count))
                                (lib/append-stage))
            [by-count new-count] (lib/aggregable-columns count-of-counts nil)]
        (is (str/includes? (->sparql (lib/aggregate count-of-counts (lib/max by-count)))
                           "SELECT (MAX(?ag_0) AS ?ag2_0)"))
        (is (str/includes? (->sparql (lib/aggregate count-of-counts (lib/max new-count)))
                           "SELECT (MAX(?ag1_0) AS ?ag2_0)"))))
    (testing "a stage that only filters keeps the aggregation's name for the next one"
      (let [filtered (-> counts (lib/filter (lib/> (count-of counts) 0)) (lib/append-stage))]
        (is (str/includes? (->sparql (lib/aggregate filtered (lib/max (count-of filtered))))
                           "SELECT (MAX(?ag_0) AS ?ag1_0)"))))
    (testing "the next stage finds each column by Lib's name, deduplicated and truncated as Lib does"
      (let [named (fn [cols nm] (first (filter #(= nm (:lib/source-column-alias %)) cols)))
            age   (tu/column q "age")]
        (testing "a name already taken moves on (count_2_2)"
          (let [two-counts (-> q
                               (lib/breakout (tu/column q lib/breakoutable-columns tu/rdfs-label))
                               (lib/aggregate (lib/count))
                               (lib/aggregate (lib/distinct age))
                               (lib/append-stage))
                regrouped  (-> two-counts
                               (lib/breakout (named (lib/breakoutable-columns two-counts) "count_2"))
                               (lib/aggregate (lib/count))
                               (lib/aggregate (lib/distinct (named (lib/aggregable-columns two-counts nil) "count")))
                               (lib/append-stage))
                sparql     (->sparql (lib/filter regrouped (lib/> (named (lib/filterable-columns regrouped) "count_2") 5)))]
            (is (str/starts-with? sparql "SELECT ?ag_1 ?ag1_0 ?ag1_1\n"))
            (is (str/includes? sparql "FILTER (?ag_1 > 5)"))
            (is (str/includes? (->sparql (lib/aggregate regrouped (lib/max (named (lib/aggregable-columns regrouped nil) "count_2_2"))))
                               "SELECT (MAX(?ag1_1) AS ?ag2_0)"))))
        (testing "names that differ only in case"
          (let [with-expr (lib/expression q "Count" (lib/+ age 1))
                counted   (-> with-expr
                              (lib/breakout (tu/column with-expr lib/breakoutable-columns "Count"))
                              (lib/aggregate (lib/count))
                              (lib/append-stage))]
            (is (str/starts-with? (->sparql (lib/breakout counted (named (lib/breakoutable-columns counted) "count_2")))
                                  "SELECT ?ag_0\n"))))
        (testing "a name longer than Lib's 60-byte limit"
          (let [long-name (str/join (repeat 70 "x"))
                with-expr (lib/expression q long-name (lib/+ age 1))
                counted   (-> with-expr
                              (lib/breakout (tu/column with-expr lib/breakoutable-columns long-name))
                              (lib/aggregate (lib/count))
                              (lib/append-stage))]
            (is (str/includes? (->sparql (lib/aggregate counted (lib/max (first (lib/aggregable-columns counted nil)))))
                               (str "SELECT (MAX(?" long-name ") AS ?ag1_0)")))))))
    (testing "a stage that drops an aggregation and reuses its name hands the next stage its own column"
      (let [renamed (-> counts
                        (lib/expression "count" (lib/* (count-of counts) 100))
                        (as-> $ (lib/with-fields $ [(tu/column $ lib/fieldable-columns tu/rdfs-label)
                                                    (lib/expression-ref $ "count")]))
                        (lib/append-stage))]
        (is (str/includes? (->sparql (lib/aggregate renamed (lib/max (count-of renamed))))
                           "SELECT (MAX(?count) AS ?ag_0)"))))
    (testing "a custom column listed before a source column of the same name takes that name"
      (let [renamed (-> counts
                        (lib/expression "count" (lib/* (count-of counts) 100))
                        (as-> $ (lib/with-fields $ [(lib/expression-ref $ "count")
                                                    (tu/column $ lib/fieldable-columns tu/rdfs-label)
                                                    (count-of counts)]))
                        (lib/append-stage))
            [expr-count _ source-count] (lib/aggregable-columns renamed nil)]
        (is (= ["count" "count_2"] (map :lib/source-column-alias [expr-count source-count])))
        (is (str/includes? (->sparql (lib/aggregate renamed (lib/max expr-count)))
                           "SELECT (MAX(?count) AS ?ag1_0)"))
        (is (str/includes? (->sparql (lib/aggregate renamed (lib/max source-count)))
                           "SELECT (MAX(?ag_0) AS ?ag1_0)"))))
    (testing "the last stage picks and reorders the source's columns by Lib's names"
      (let [label-col  #(tu/column % lib/fieldable-columns tu/rdfs-label)
            picked     (fn [stage cols] (->sparql (lib/with-fields stage cols)))
            with-count (fn [base] (-> base (lib/aggregate (lib/count)) (lib/append-stage)))
            by-expr    (let [e (lib/expression q "Count" (lib/+ (tu/column q "age") 1))]
                         (with-count (lib/breakout e (tu/column e lib/breakoutable-columns "Count"))))]
        (is (str/starts-with? (picked counts [(count-of counts)]) "SELECT ?ag_0\n"))
        (is (str/starts-with? (picked counts [(count-of counts) (label-col counts)])
                              "SELECT ?ag_0 ?http___www_w3_org_2000_01_rdf_schema_label\n"))
        (is (str/starts-with? (picked by-expr [(second (lib/fieldable-columns by-expr))]) "SELECT ?ag_0\n"))))
    (testing "the prefix skips one a custom column of the same stage uses"
      (is (str/includes? (->sparql (-> counts
                                       (lib/expression "ag1_0" (lib/* (count-of counts) 2))
                                       (lib/aggregate (lib/count))))
                         "(COUNT(*) AS ?ag2_0)")))
    (testing "the prefix skips one a native source already projects"
      (is (str/includes? (->sparql (-> (lib/native-query tu/provider
                                                         (str "PREFIX ex: <https://example.org/>\n"
                                                              "SELECT (COUNT(?s) AS ?ag_0) WHERE { ?s a ex:Person }"))
                                       (lib/append-stage)
                                       (lib/aggregate (lib/count))))
                         "SELECT (COUNT(*) AS ?ag1_0)")))))

(deftest temporal-filters-test
  (let [q     (tu/person-query)
        born  (tu/column q "birthDate")
        upd   (tu/column q "updated")
        xsd   "<http://www.w3.org/2001/XMLSchema#"]
    (testing "date strings from the UI arrive as typed xsd:date bounds"
      (is (str/includes? (->sparql (lib/filter q (lib/between born "1990-01-01" "1999-12-31")))
                         (str "?birthDate >= \"1990-01-01\"^^" xsd "date>"))))
    (testing "a dateTime bound is compared in the form matching each value's TZ()"
      (let [sparql (->sparql (lib/filter q (lib/< upd "2024-01-01")))]
        (is (str/includes? sparql "TZ(?updated) != \"\""))
        (is (str/includes? sparql (str "?updated < \"2024-01-01T00:00:00\"^^" xsd "dateTime>")))))
    (testing "relative presets (desugared by the QP) compile for every unit"
      (doseq [unit [:day :week :month :quarter :year]]
        (is (str/includes? (->sparql (lib/filter q (lib/time-interval born -2 unit)))
                           "?birthDate >=")
            (name unit))))))

(deftest custom-columns-test
  (let [q      (tu/person-query)
        label  (tu/column q tu/rdfs-label)
        age    (tu/column q "age")
        shout  (lib/expression q "shout" (lib/upper label))
        ref    #(lib/expression-ref % "shout")]
    (testing "a custom column is bound and projected after Lib's columns"
      (let [sparql (->sparql shout)]
        (is (re-find #"BIND\(UCASE\(STR\(\?\w+\)\) AS \?shout\)" sparql))
        (is (= "shout" (last (select-vars sparql))))
        (is (= 8 (count (select-vars sparql))))))
    (testing "filter and order-by reference the custom column's variable"
      (let [sparql (->sparql (-> shout
                                 (lib/filter (lib/= (ref shout) "ALICE"))
                                 (lib/order-by (ref shout) :desc)))]
        (is (str/includes? sparql "FILTER (STR(?shout) = \"ALICE\")"))
        (is (str/includes? sparql "ORDER BY DESC(?shout)"))))
    (testing "aggregating and grouping by a custom column"
      (let [dbl    (lib/expression q "double" (lib/* age 2))
            sparql (->sparql (-> dbl
                                 (lib/aggregate (lib/sum (lib/expression-ref dbl "double")))))]
        (is (str/includes? sparql "BIND((?age * 2) AS ?double)"))
        (is (str/includes? sparql "(SUM(?double) AS ?ag_0)")))
      (let [sparql (->sparql (-> shout (lib/breakout (ref shout)) (lib/aggregate (lib/count))))]
        (is (str/includes? sparql "GROUP BY ?shout"))))
    (testing "a later stage reads the column through the sub-SELECT and can build on it"
      (let [outer  (lib/append-stage shout)
            inner  (tu/column outer lib/visible-columns "shout")
            sparql (->sparql (lib/expression outer "whisper" (lib/lower inner)))]
        (is (str/includes? sparql "BIND(LCASE(STR(?shout)) AS ?whisper)"))
        (is (= ["shout" "whisper"] (take-last 2 (select-vars sparql))))
        (is (= 9 (count (select-vars sparql))))))))

(deftest saved-native-question-as-source-test
  (testing "the saved SPARQL is the sub-SELECT, with its prologue at the top"
    (let [q   (tu/native-card-query)
          age (tu/column q lib/filterable-columns "age")]
      (doseq [[label query] {"no outer clauses"  q
                             "filter and limit"  (-> q (lib/filter (lib/between age 20 40)) (lib/limit 10))
                             "count by a column" (-> q (lib/aggregate (lib/count)) (lib/breakout age))}]
        (testing label
          (let [sparql (->sparql query)]
            (is (not (str/includes? sparql "<>")) sparql)
            (is (str/includes? sparql "?s a ex:Person") sparql)
            (is (str/starts-with? sparql "PREFIX ex:") sparql))))))
  (testing "the outer clauses read the native columns"
    (let [q      (tu/native-card-query)
          age    (tu/column q lib/filterable-columns "age")
          sparql (->sparql (-> q (lib/filter (lib/between age 20 40)) (lib/limit 10)))]
      (is (str/includes? sparql "?age >= 20") sparql)
      (is (= ["name" "age"] (select-vars sparql)))))
  (testing "a comment and BASE in the prologue, and a comment ending the query"
    (let [sparql (->sparql (tu/native-card-query 3))]
      (is (str/starts-with? sparql "# people and their ages\nBASE <https://example.org/>\nPREFIX ex:") sparql)
      (is (str/includes? sparql "# no LIMIT\n") sparql)))
  (testing "FROM clauses move out of the sub-SELECT, before the outer WHERE"
    (let [sparql (->sparql (tu/native-card-query 4))]
      (is (re-find #"(?m)^FROM <https://example.org/g>\nFROM NAMED ex:h\nWHERE \{" sparql) sparql)
      (is (= 2 (count (re-seq #"FROM" sparql))) sparql)))
  (testing "a non-ASCII native variable keeps its name"
    (let [q      (tu/native-card-query 5)
          idade  (tu/column q lib/filterable-columns "idade_média")
          sparql (->sparql (-> q (lib/aggregate (lib/count)) (lib/breakout idade)))]
      (is (str/includes? sparql "GROUP BY ?idade_média") sparql)))
  (testing "a non-ASCII native variable keeps its name two stages up"
    (let [q      (tu/native-card-query 8)
          idade  (tu/column q lib/filterable-columns "idade_média")
          sparql (->sparql (-> q (lib/filter (lib/> idade 3)) (lib/aggregate (lib/count)) (lib/breakout idade)))]
      (is (not (str/includes? sparql "idade_m_dia")) sparql)
      (is (str/includes? sparql "?idade_média > 3") sparql)))
  (testing "a DEFINE pragma stays in the prologue"
    ;; not parse-checked: DEFINE is Virtuoso's, not SPARQL 1.1
    (let [sparql (:query (tu/compile-query (tu/native-card-query 6)))]
      (is (str/starts-with? sparql "DEFINE input:inference \"r\"\nPREFIX ex:") sparql)))
  (testing "without recorded columns, as the tile map runs it, the SELECT clause names the variables"
    (let [stage #'metabase.driver.sparql.mbql/compile-native-stage]
      (is (= ["whs" "latitude" "longitude"]
             (:vars (stage (str "SELECT DISTINCT ?whs (SAMPLE(STR(?lat)) AS ?latitude) ( SAMPLE(?lon) AS ?longitude )"
                                " FROM <https://example.org/g> WHERE { ?s ?p ?o } GROUP BY ?whs")
                           nil))))
      (is (= ["idade_média"] (:vars (stage "SELECT ?idade_média WHERE { ?s ?p ?idade_média }" []))))))
  (testing "a SELECT * source without recorded columns fails with a clear error"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"has not recorded the columns"
                          (tu/compile-query (tu/native-card-query 7)))))
  (testing "a non-SELECT source fails with a clear error"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"needs a SELECT query as its source"
                          (tu/compile-query (tu/native-card-query 2))))))

(deftest page-test
  (testing "lib/with-page compiles to LIMIT and OFFSET (the agent API paginates with it)"
    (let [q (tu/person-query)]
      (is (str/ends-with? (->sparql (lib/with-page q {:page 1 :items 5})) "LIMIT 5 OFFSET 0"))
      (is (str/ends-with? (->sparql (lib/with-page q {:page 3 :items 5})) "LIMIT 5 OFFSET 10"))
      (is (str/ends-with? (->sparql (lib/limit q 7)) "LIMIT 7"))))
  (testing "on a question over a saved native question, the outer query is paged"
    (is (str/ends-with? (->sparql (lib/with-page (tu/native-card-query) {:page 2 :items 5}))
                        "LIMIT 5 OFFSET 5"))))
