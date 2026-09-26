(ns metabase.driver.sparql.connection-test
  "Unit tests for endpoint connection checks."
  (:require [clojure.test :refer :all]
            [metabase.driver.sparql.connection :as connection]
            [metabase.driver.sparql.execute :as execute]))

(def ^:private database {:details {:endpoint "http://example.org/sparql"}})

(deftest dbms-version-test
  (testing "reads the version from the first binding row"
    (with-redefs [execute/execute-sparql-query
                  (fn [_ _ _]
                    [true {:results {:bindings [{:version {:type "literal" :value "SPARQL 1.1"}}]}}])]
      (is (= {:version "SPARQL 1.1"}
             (connection/dbms-version :sparql database)))))

  (testing "falls back to SPARQL 1.0 when both probes fail"
    (with-redefs [execute/execute-sparql-query
                  (fn [_ _ _] [false "SPARQL endpoint returned status: 400" :query])]
      (is (= {:version "SPARQL 1.0"}
             (connection/dbms-version :sparql database))))))
