(ns metabase.driver.sparql.e2e-test
  "End-to-end tests: real pMBQL queries compiled by driver/mbql->native and run
  through driver/execute-reducible-query against the live SPARQL endpoint from
  the smoke harness (make smoke). Covers the full pipeline the unit tests skip:
  returned-columns/->legacy-MBQL, the QP-store detail lookup, and conversion of
  live results into rows and column metadata."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [metabase.driver-api.core :as driver-api]
            [metabase.driver.sparql.database :as database]
            [metabase.driver.sparql.test-util :as tu]
            [metabase.lib.core :as lib]))

(use-fixtures :once tu/skip-without-live-endpoint)

(deftest ^:integration provider-matches-live-schema-test
  (testing "every provider column exists in describe-table's live view of the
            fixture, so the other tests exercise sync-discoverable fields
            (the provider is a deliberate subset: sync also finds rdf:type)"
    (let [live-names (->> (database/describe-table :sparql tu/db {:name "Person"})
                          :fields
                          (map :name)
                          set)]
      (is (set/subset? (set (map :name (lib/fieldable-columns (tu/person-query))))
                       live-names)))))

(deftest ^:integration fields-projection-rows-and-types-test
  (let [q     (tu/person-query)
        label (tu/column q tu/rdfs-label)
        age   (tu/column q "age")
        {:keys [cols rows]} (tu/run-query (lib/with-fields q [label age]))]
    ;; exactly 2 columns proves returned-columns drove the projection: the
    ;; compiler's fallback path would project ?subject as well.
    (is (= 2 (count cols)))
    (is (= [:type/Text :type/Integer] (mapv :base_type cols)))
    (is (= #{["Alice" 30] ["Bob" 25]} (set rows)))
    (is (every? #(instance? Long (second %)) rows))))

(deftest ^:integration default-projection-includes-subject-test
  (let [{:keys [cols rows]} (tu/run-query (tu/person-query))]
    (is (= 4 (count cols)))
    (is (= :type/URL (:base_type (first cols))))
    (is (= #{"https://example.org/alice" "https://example.org/bob"}
           (set (map first rows))))))

(deftest ^:integration filter-equals-label-test
  (let [q     (tu/person-query)
        label (tu/column q tu/rdfs-label)
        q     (-> q
                  (lib/with-fields [label])
                  (lib/filter (lib/= label "Alice")))
        {:keys [rows]} (tu/run-query q)]
    (is (= [["Alice"]] rows))))

(deftest ^:integration filter-between-age-test
  (let [q     (tu/person-query)
        label (tu/column q tu/rdfs-label)
        age   (tu/column q "age")
        q     (-> q
                  (lib/with-fields [label age])
                  (lib/filter (lib/between age 26 35)))
        {:keys [rows]} (tu/run-query q)]
    (is (= [["Alice" 30]] rows))))

(deftest ^:integration order-by-limit-test
  (let [q     (tu/person-query)
        label (tu/column q tu/rdfs-label)
        age   (tu/column q "age")
        q     (-> q
                  (lib/with-fields [label age])
                  (lib/order-by age :desc)
                  (lib/limit 1))
        {:keys [rows]} (tu/run-query q)]
    (is (= [["Alice" 30]] rows))))

(deftest ^:integration count-aggregation-test
  (let [q (lib/aggregate (tu/person-query) (lib/count))
        {:keys [cols rows native]} (tu/run-query q)]
    ;; DISTINCT-ness is unobservable in a 2-row fixture, so anchor it in the
    ;; generated SPARQL.
    (is (str/includes? native "COUNT(DISTINCT"))
    (is (= [:type/Integer] (mapv :base_type cols)))
    (is (= [[2]] rows))
    (is (instance? Long (ffirst rows)))))

(deftest ^:integration count-by-breakout-test
  (let [q     (tu/person-query)
        label (tu/column q lib/breakoutable-columns tu/rdfs-label)
        q     (-> q
                  (lib/breakout label)
                  (lib/aggregate (lib/count)))
        {:keys [rows]} (tu/run-query q)]
    (is (= #{["Alice" 1] ["Bob" 1]} (set rows)))))

(deftest ^:integration derived-stage-aggregation-filter-test
  ;; drill-through shape: an outer stage filtering the inner stage's
  ;; aggregation result by its Lib name (resolved via expected-name->var).
  (let [base      (let [q (tu/person-query)]
                    (-> q
                        (lib/breakout (tu/column q lib/breakoutable-columns tu/rdfs-label))
                        (lib/aggregate (lib/count))
                        (lib/append-stage)))
        count-col (tu/column base lib/filterable-columns "count")]
    (testing "filter that keeps every group"
      (let [{:keys [rows]} (tu/run-query (lib/filter base (lib/> count-col 0)))]
        (is (= #{["Alice" 1] ["Bob" 1]} (set rows)))))
    (testing "filter that excludes every group"
      (let [{:keys [cols rows]} (tu/run-query (lib/filter base (lib/> count-col 5)))]
        ;; a successful empty result still carries the SELECT vars as columns
        ;; (the error path throws instead of responding) — don't pass on failure.
        (is (= 2 (count cols)))
        (is (= [] rows))))))

(deftest ^:integration native-ask-returns-boolean-test
  (let [{:keys [cols rows]} (tu/run-native "ASK { ?s a <https://example.org/Person> }")]
    (is (= [{:name "boolean" :display_name "boolean" :base_type :type/Boolean}]
           cols))
    (is (= [[true]] rows))))

(deftest ^:integration native-syntax-error-raises-test
  (testing "a malformed native query raises an invalid-query error carrying the
            endpoint's message, instead of succeeding with an empty result"
    (let [e (is (thrown? clojure.lang.ExceptionInfo
                         (tu/run-native "SELECT ?x WHERE {")))]
      (is (= driver-api/qp.error-type.invalid-query (:type (ex-data e))))
      (is (str/includes? (ex-message e) "Error executing SPARQL query")))))
