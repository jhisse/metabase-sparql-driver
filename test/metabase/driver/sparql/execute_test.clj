(ns metabase.driver.sparql.execute-test
  "Unit tests for query execution. The HTTP layer and the metadata provider are
   stubbed with `with-redefs`, so no network or app DB is touched."
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
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

(deftest process-response-classifies-errors-test
  (let [process @#'execute/process-response]
    (testing "a 400 (query rejection) is kind :query"
      (is (= :query (nth (process {:status 400 :body "parse error"}) 2))))
    (testing "auth failures are kind :db"
      (doseq [status [401 403 407]]
        (is (= :db (nth (process {:status status :body "denied"}) 2))
            (str "status " status))))
    (testing "server-side 5xx is kind :db"
      (doseq [status [500 502 503 504]]
        (is (= :db (nth (process {:status status :body "boom"}) 2))
            (str "status " status))))
    (testing "a 200 with an unparseable body is kind :db (endpoint problem, not the query)"
      (let [[success msg kind] (process {:status 200 :body "<html>login page</html>"})]
        (is (false? success))
        (is (str/includes? msg "Invalid JSON response"))
        (is (= :db kind))))
    (testing "a huge error body is truncated before being embedded in the message"
      (let [big (str/join (repeat 100000 "x"))
            [_ msg _] (process {:status 500 :body big})]
        (is (< (count msg) 1200))
        (is (str/includes? msg "(truncated)"))))))

(deftest execute-reducible-query-raises-query-rejections-test
  (testing "a query rejection surfaces as an invalid-query ex-info, not an empty result"
    (with-execution-stubs [false "SPARQL endpoint returned status: 400\nBody: parse error" :query]
      (let [e (is (thrown-with-msg? ExceptionInfo #"parse error"
                                    (execute/execute-reducible-query
                                     {:native {:query "SELECT ?x WHERE {"}} nil
                                     (fn [& _] (is false "respond must not be called on failure")))))]
        (is (= driver-api/qp.error-type.invalid-query (:type (ex-data e))))))))

(deftest execute-reducible-query-raises-endpoint-problems-as-db-test
  (testing "an endpoint problem (auth/5xx/bad body) surfaces as a db-type ex-info"
    (with-execution-stubs [false "SPARQL endpoint returned status: 503\nBody: unavailable" :db]
      (let [e (is (thrown-with-msg? ExceptionInfo #"unavailable"
                                    (execute/execute-reducible-query
                                     {:native {:query "ASK { }"}} nil
                                     (fn [& _] (is false "respond must not be called on failure")))))]
        (is (= driver-api/qp.error-type.db (:type (ex-data e))))))))

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
