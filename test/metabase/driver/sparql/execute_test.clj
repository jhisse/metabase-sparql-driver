(ns metabase.driver.sparql.execute-test
  "Unit tests for query execution. The HTTP layer and the metadata provider are
   stubbed with `with-redefs`, so no network or app DB is touched."
  (:require [clojure.test :refer :all]
            [metabase.driver-api.core :as driver-api]
            [metabase.driver.sparql.execute :as execute])
  (:import [clojure.lang ExceptionInfo]))

(def ^:private fake-db
  {:details {:endpoint "http://sparql.invalid/query"}})

(defmacro ^:private with-execution-stubs
  "Run `body` with the metadata provider stubbed to `fake-db` and
   `execute-sparql-query` returning `query-result`."
  [query-result & body]
  `(with-redefs [driver-api/metadata-provider (constantly ::provider)
                 driver-api/database          (fn [_#] fake-db)
                 execute/execute-sparql-query (fn [_# _# _#] ~query-result)]
     (let [res# (do ~@body)] res#)))

(deftest execute-reducible-query-raises-endpoint-errors-test
  (testing "an endpoint-returned error surfaces as an invalid-query ex-info, not an empty result"
    (with-execution-stubs [false "SPARQL endpoint returned status: 400\nBody: parse error"]
      (let [e (is (thrown-with-msg? ExceptionInfo #"parse error"
                                    (execute/execute-reducible-query
                                     {:native {:query "SELECT ?x WHERE {"}} nil
                                     (fn [& _] (is false "respond must not be called on failure")))))]
        (is (= driver-api/qp.error-type.invalid-query (:type (ex-data e))))))))

(deftest execute-reducible-query-raises-transport-errors-test
  (testing "a transport failure surfaces as a db-type ex-info"
    (with-execution-stubs [false "Connection refused" :transport]
      (let [e (is (thrown-with-msg? ExceptionInfo #"Connection refused"
                                    (execute/execute-reducible-query
                                     {:native {:query "ASK { }"}} nil
                                     (fn [& _] (is false "respond must not be called on failure")))))]
        (is (= driver-api/qp.error-type.db (:type (ex-data e))))))))

(deftest execute-reducible-query-success-path-still-responds-test
  (testing "a successful query still flows through process-query-results into respond"
    (with-execution-stubs [true {:head    {:vars ["x"]}
                                 :results {:bindings [{:x {:type "literal" :value "1"}}]}}]
      (let [responded (atom nil)]
        (execute/execute-reducible-query
         {:native {:query "SELECT ?x WHERE { ?s ?p ?x }"}} nil
         (fn [metadata rows] (reset! responded {:metadata metadata :rows (mapv vec rows)})))
        (is (some? @responded))
        (is (= [["1"]] (:rows @responded)))))))
