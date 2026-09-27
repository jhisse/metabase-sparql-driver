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

  (testing "uses the VALUES probe when the BIND probe is rejected"
    (with-redefs [execute/execute-sparql-query
                  (fn [_ query _]
                    (if (re-find #"(?i)\bVALUES\b" query)
                      [true {:results {:bindings [{:version {:type "literal" :value "SPARQL 1.1"}}]}}]
                      [false "SPARQL endpoint returned status: 400" :query]))]
      (is (= {:version "SPARQL 1.1"}
             (connection/dbms-version :sparql database)))))

  (testing "falls back to SPARQL 1.0 when both probes fail"
    (with-redefs [execute/execute-sparql-query
                  (fn [_ _ _] [false "SPARQL endpoint returned status: 400" :query])]
      (is (= {:version "SPARQL 1.0"}
             (connection/dbms-version :sparql database))))))

(deftest can-connect?-test
  (testing "a successful probe returns true, with the short probe timeout"
    (let [opts (atom nil)]
      (with-redefs [execute/execute-sparql-query (fn [_ _ o] (reset! opts o) [true {:boolean true}])]
        (is (true? (connection/can-connect? "http://example.org/sparql" {})))
        (is (= 30000 (:read-timeout-ms @opts))))))
  (testing "a failed probe throws with the endpoint's reason, for the connection form to show"
    (with-redefs [execute/execute-sparql-query (fn [_ _ _] [false "SPARQL endpoint returned status: 401" :db])]
      (is (thrown-with-msg? Exception #"Connection failed: SPARQL endpoint returned status: 401"
                            (connection/can-connect? "http://example.org/sparql" {}))))))
