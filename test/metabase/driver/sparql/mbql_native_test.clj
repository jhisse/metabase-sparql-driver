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
      (is (= 6 (count (select-vars sparql))))
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
        (is (= 7 (count (select-vars sparql))))))
    (testing "filter and order-by reference the custom column's variable"
      (let [sparql (->sparql (-> shout
                                 (lib/filter (lib/= (ref shout) "ALICE"))
                                 (lib/order-by (ref shout) :desc)))]
        (is (str/includes? sparql "FILTER (?shout = \"ALICE\")"))
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
        (is (= 8 (count (select-vars sparql))))))))
