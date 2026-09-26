(ns metabase.driver.sparql.execute-test
  "Unit tests for query execution. The HTTP layer and the metadata provider are
   stubbed with `with-redefs`, so no network or app DB is touched."
  (:require [clj-http.client :as http]
            [clojure.string :as str]
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

(defn- capture-post
  "Call execute-sparql-query with http/post stubbed to `respond`; returns
  {:result … :url … :opts …} with the request clj-http would have sent."
  ([options] (capture-post options (fn [_ _] {:status 200 :body "{\"boolean\":true}"})))
  ([options respond]
   (let [req (atom nil)]
     (with-redefs [http/post (fn [url opts] (reset! req {:url url :opts opts}) (respond url opts))]
       (let [result (execute/execute-sparql-query "http://sparql.invalid/query" "ASK {}" options)]
         (assoc @req :result result))))))

(deftest execute-sparql-query-request-shape-test
  (testing "the query goes in a POST form body and JSON results are requested"
    (let [{:keys [url opts result]} (capture-post {})]
      (is (= "http://sparql.invalid/query" url))
      (is (= {:query "ASK {}"} (:form-params opts)))
      (is (= :json (:accept opts)))
      (is (false? (:throw-exceptions opts)) "non-200s must reach process-response, not throw")
      (is (= [true {:boolean true}] result))))
  (testing "no options: no default graph, TLS verification on, no credentials"
    (let [{:keys [opts]} (capture-post {})]
      (is (not (contains? opts :query-params)))
      (is (not (contains? opts :insecure?)))
      (is (not (contains? opts :basic-auth)))
      (is (not (contains? opts :headers)))))
  (testing "default graph travels as the default-graph-uri protocol parameter"
    (is (= {:default-graph-uri "https://example.org/"}
           (:query-params (:opts (capture-post {:default-graph "https://example.org/"}))))))
  (testing "use-insecure disables TLS verification"
    (is (true? (:insecure? (:opts (capture-post {:insecure? true}))))))
  (testing "auth fragments from auth/http-options are merged into the request"
    (is (= ["alice" "secret"]
           (:basic-auth (:opts (capture-post {:auth {:basic-auth ["alice" "secret"]}})))))
    (is (= {"Authorization" "Bearer t0k"}
           (:headers (:opts (capture-post {:auth {:headers {"Authorization" "Bearer t0k"}}})))))))

(deftest execute-sparql-query-transport-failure-test
  (testing "an exception from the HTTP client becomes [false message :transport]"
    (is (= [false "Connection refused" :transport]
           (:result (capture-post {} (fn [_ _] (throw (java.net.ConnectException. "Connection refused")))))))))

(deftest execute-reducible-query-uses-configured-endpoint-and-auth-test
  (testing "the endpoint and credentials always come from the database details;
            a native query cannot redirect stored credentials elsewhere"
    (let [req (atom nil)]
      (with-redefs [driver-api/metadata-provider (constantly ::provider)
                    driver-api/database          (fn [_] {:details {:endpoint      "http://configured.invalid/query"
                                                                    :default-graph "https://example.org/"
                                                                    :auth-type     "basic"
                                                                    :auth-username "alice"
                                                                    :auth-password "secret"}})
                    http/post                    (fn [url opts]
                                                   (reset! req {:url url :opts opts})
                                                   {:status 200 :body "{\"boolean\":true}"})]
        (execute/execute-reducible-query
         {:endpoint "http://attacker.invalid/"
          :native   {:query "ASK {}" :endpoint "http://attacker.invalid/"}}
         nil
         (fn [_ rows] (vec rows)))
        (is (= "http://configured.invalid/query" (:url @req)))
        (is (= ["alice" "secret"] (:basic-auth (:opts @req))))
        (is (= {:default-graph-uri "https://example.org/"} (:query-params (:opts @req))))))))
