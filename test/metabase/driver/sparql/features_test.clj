(ns metabase.driver.sparql.features-test
  "Unit tests for the features the driver advertises through
   driver/database-supports? — the multimethod Metabase actually calls."
  (:require [clojure.test :refer :all]
            [metabase.driver :as driver]
            [metabase.driver.sparql]
            [metabase.driver.sparql.execute :as execute]))

(def ^:private supported-features
  "Every static feature the driver reports as supported. A flag enables MBQL
   clauses in the UI, so changing this set is deliberate: first confirm the
   compiler handles every clause the feature gates (see AGENTS.md)."
  #{:basic-aggregations
    :case-sensitivity-string-filter-options
    :describe-fks
    :expressions
    :expressions/float
    :expressions/integer
    :expressions/text
    :left-join
    :native-parameters
    :metadata/key-constraints
    :regex
    :regex/lookaheads-and-lookbehinds
    :test/cannot-destroy-db
    ;; not in the driver's table: inherited from Metabase's default
    :test/create-table-without-data})

(def ^:private db {:details {:endpoint "http://sparql.invalid/query"}})

(deftest static-feature-flags-test
  (testing "the advertised feature set is exactly the reviewed one, and no static flag hits the endpoint"
    (with-redefs [execute/execute-sparql-query (fn [& _] (throw (AssertionError. "static flag queried the endpoint")))]
      (is (= supported-features
             (set (filter #(driver/database-supports? :sparql % db)
                          (disj driver/features :now))))))))

(deftest now-feature-probe-test
  (let [probe (fn [result]
                (with-redefs [execute/execute-sparql-query (fn [_ _ _] result)]
                  (driver/database-supports? :sparql :now db)))]
    (testing "supported when the endpoint returns a NOW() binding"
      (is (true? (probe [true {:results {:bindings [{:currentDateTime {:type "literal" :value "2026-01-01T00:00:00Z"}}]}}]))))
    (testing "unsupported on an empty result"
      (is (false? (probe [true {:results {:bindings []}}]))))
    (testing "unsupported when the probe query fails"
      (is (false? (probe [false "SPARQL endpoint returned status: 400" :query]))))))
