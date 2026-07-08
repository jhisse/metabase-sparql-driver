(ns metabase.driver.sparql.smoke-test
  "Smoke / integration tests that exercise the driver's HTTP + schema-sync layer
   against a real SPARQL endpoint.

   These are tagged `^:integration` and are EXCLUDED from the hermetic `make test`
   run. They are driven by `make smoke`, which starts an ephemeral Oxigraph
   endpoint (docker-compose.test.yml), seeds it with test/resources/fixtures/smoke.ttl
   into the named graph <https://example.org/>, and points `SPARQL_TEST_ENDPOINT`
   here. Run directly with:

     SPARQL_TEST_ENDPOINT=http://localhost:7878/query clojure -X:test :includes '[:integration]'"
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [metabase.driver.sparql.connection :as connection]
            [metabase.driver.sparql.database :as database]
            [metabase.driver.sparql.execute :as execute]
            [metabase.driver.sparql.test-util :as tu]))

(use-fixtures :once tu/skip-without-live-endpoint)

(defn- binding-values
  "Pull the `:value`s of variable `var-kw` out of a SPARQL results map."
  [result var-kw]
  (->> (get-in result [:results :bindings])
       (map #(get-in % [var-kw :value]))))

(deftest ^:integration execute-select-returns-bindings-test
  (testing "a well-formed SELECT against the live endpoint returns the fixture rows"
    (let [q "SELECT ?label WHERE { ?s a <https://example.org/Person> ; <http://www.w3.org/2000/01/rdf-schema#label> ?label }"
          [success result] (execute/execute-sparql-query tu/endpoint q (:details tu/db))]
      (is (true? success))
      (is (= #{"Alice" "Bob"} (set (binding-values result :label)))))))

(deftest ^:integration execute-malformed-query-fails-gracefully-test
  (testing "a syntactically invalid query yields [false message], not an exception"
    (let [[success result] (execute/execute-sparql-query tu/endpoint "SELECT ?x WHERE {" (:details tu/db))]
      (is (false? success))
      (is (string? result)))))

(deftest ^:integration can-connect-succeeds-against-live-endpoint-test
  (testing "can-connect? returns true for a reachable SPARQL endpoint"
    (is (true? (connection/can-connect? tu/endpoint {})))))

(deftest ^:integration can-connect-throws-for-unreachable-endpoint-test
  (testing "can-connect? throws when the endpoint cannot be reached"
    (is (thrown? Exception
                 (connection/can-connect? "http://localhost:1/query" {})))))

(deftest ^:integration describe-database-discovers-classes-test
  (testing "describe-database discovers the fixture's RDF classes as tables"
    (let [table-names (->> (:tables (database/describe-database :sparql tu/db))
                           (map :name)
                           set)]
      (is (contains? table-names "Person"))
      (is (contains? table-names "Company")))))

(deftest ^:integration describe-table-discovers-fields-test
  (testing "describe-table discovers the synthetic PK plus the class's properties"
    (let [field-names (->> (:fields (database/describe-table :sparql tu/db {:name "Person"}))
                           (map :name)
                           set)]
      (testing "the synthetic subject primary key is always present"
        (is (contains? field-names "subject")))
      (testing "same-graph properties are shortened to their local names"
        (is (contains? field-names "age"))
        (is (contains? field-names "knows")))
      (testing "rdfs:label is discovered too (kept as its full foreign URI)"
        (is (some #(str/includes? % "rdf-schema#label") field-names))))))
