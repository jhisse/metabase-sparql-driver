(ns metabase.driver.sparql.smoke-test
  "Smoke / integration tests that exercise the driver's HTTP + schema-sync layer
   against a real SPARQL endpoint.

   These are tagged `^:integration` and are EXCLUDED from the hermetic `make test`
   run. They are driven by `make smoke`, which starts an ephemeral Oxigraph
   endpoint (docker-compose.test.yml), seeds it with test/resources/fixtures/smoke.ttl
   into the named graph <https://example.org/> (and smoke-shapes.ttl into
   <https://example.org/shapes> for the SHACL sync tests), and points `SPARQL_TEST_ENDPOINT`
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
      (is (contains? table-names "Company"))
      (is (contains? table-names "City")))))

(deftest ^:integration describe-table-discovers-fields-test
  (testing "describe-table discovers the synthetic PK plus the class's properties"
    (let [fields      (:fields (database/describe-table :sparql tu/db {:name "Person"}))
          field-names (set (map :name fields))
          by-name     (into {} (map (juxt :name identity)) fields)]
      (testing "the synthetic subject primary key is always present"
        (is (contains? field-names "subject")))
      (testing "same-graph properties are shortened to their local names"
        (is (contains? field-names "age"))
        (is (contains? field-names "knows")))
      (testing "rdfs:label is discovered too (kept as its full foreign URI)"
        (is (some #(str/includes? % "rdf-schema#label") field-names)))
      (testing "IRI-valued properties get the \"uri\" database-type; literal ones stay \"string\""
        (is (= "uri" (:database-type (by-name "knows"))))
        (is (= "string" (:database-type (by-name "age"))))))))

(deftest ^:integration shacl-describe-database-test
  (testing "the shacl strategy takes its tables from the shapes served by the endpoint"
    (let [tables (:tables (database/describe-database :sparql tu/shacl-db))
          by-name (into {} (map (juxt :name identity)) tables)]
      (is (= #{"Person" "Company" "City"} (set (keys by-name))))
      (is (= "A company that employs people" (:description (by-name "Company")))))))

(deftest ^:integration shacl-describe-table-test
  (testing "field types come from sh:datatype, IRI links from sh:class"
    (let [fields  (:fields (database/describe-table :sparql tu/shacl-db {:name "Company"}))
          by-name (into {} (map (juxt :name identity)) fields)]
      (is (= {"subject"      :type/Text
              tu/rdfs-label  :type/Text
              "foundedOn"    :type/Date
              "employees"    :type/Integer
              "revenue"      :type/Float
              "listed"       :type/Boolean
              "headquarters" :type/Text}
             (update-vals by-name :base-type)))
      (is (= "uri" (:database-type (by-name "headquarters"))))
      (is (= :type/FK (:semantic-type (by-name "headquarters"))))
      (is (true? (:database-required (by-name tu/rdfs-label)))))))

(deftest ^:integration shacl-fks-test
  (testing "sh:class property shapes become FKs to the target class's subject"
    (is (= #{["Person" "knows" "Person"]
             ["Person" "worksFor" "Company"]
             ["Company" "headquarters" "City"]}
           (set (map (juxt :fk-table-name :fk-column-name :pk-table-name)
                     (database/fks tu/shacl-db)))))))
