(ns metabase.driver.sparql.database-test
  "Unit tests for SPARQL metadata-sync helpers: URI shortening, the implicit
   Default-Graph base prefix, foreign-URI hiding, explicit/none sync strategies,
   and the SHACL shape -> Metabase metadata conversion."
  (:require [clojure.test :refer :all]
            [metabase.driver.sparql.database :as database]))

(def ^:private extract-class-name @#'database/extract-class-name)
(def ^:private shorten-uri @#'database/shorten-uri)
(def ^:private foreign-uri? @#'database/foreign-uri?)
(def ^:private absolute-uri @#'database/absolute-uri)
(def ^:private parse-schema-config @#'database/parse-schema-config)
(def ^:private build-pk-field @#'database/build-pk-field)
(def ^:private build-field-from-uri @#'database/build-field-from-uri)
(def ^:private shacl-prop->field @#'database/shacl-prop->field)
(def ^:private shacl-shape->table @#'database/shacl-shape->table)
(def ^:private shacl-shape->describe-table @#'database/shacl-shape->describe-table)

(def ^:private graph "https://odis.q.libis.be/")

(deftest extract-class-name-test
  (testing "local name is taken after the last slash or hash"
    (is (= "Persoon" (extract-class-name "https://odis.q.libis.be/Persoon")))
    (is (= "Person"  (extract-class-name "http://xmlns.com/foaf/0.1/Person")))
    (is (= "name"    (extract-class-name "http://example.org/schema#name")))))

(deftest shorten-uri-test
  (testing "the Default-Graph prefix is stripped"
    (is (= "Persoon" (shorten-uri (str graph "Persoon") graph))))
  (testing "URIs outside the Default Graph are left untouched"
    (is (= "http://xmlns.com/foaf/0.1/Person"
           (shorten-uri "http://xmlns.com/foaf/0.1/Person" graph))))
  (testing "a blank Default Graph is a no-op"
    (is (= (str graph "Persoon") (shorten-uri (str graph "Persoon") ""))))
  (testing "stripping never yields a blank name (uri == default-graph)"
    (is (= graph (shorten-uri graph graph)))))

(deftest foreign-uri?-test
  (is (true?  (foreign-uri? "http://xmlns.com/foaf/0.1/Person" graph)))
  (is (false? (foreign-uri? (str graph "Persoon") graph)))
  (testing "without a Default Graph nothing is considered foreign"
    (is (false? (foreign-uri? "http://xmlns.com/foaf/0.1/Person" "")))))

(deftest absolute-uri-test
  (testing "relative names are resolved against the Default Graph"
    (is (= (str graph "naam") (absolute-uri "naam" graph))))
  (testing "already-absolute URIs are returned unchanged"
    (is (= "http://xmlns.com/foaf/0.1/name" (absolute-uri "http://xmlns.com/foaf/0.1/name" graph))))
  (testing "blank inputs are no-ops"
    (is (= "" (absolute-uri "" graph)))
    (is (= "naam" (absolute-uri "naam" "")))))

(deftest parse-schema-config-test
  (testing "valid JSON is decoded with keyword keys"
    (is (= {:tables [{:name "https://odis.q.libis.be/Persoon"}]}
           (parse-schema-config "{\"tables\":[{\"name\":\"https://odis.q.libis.be/Persoon\"}]}"))))
  (testing "blank config yields nil"
    (is (nil? (parse-schema-config "")))
    (is (nil? (parse-schema-config nil))))
  (testing "invalid JSON is swallowed and yields nil"
    (is (nil? (parse-schema-config "{not-json")))))

(deftest build-pk-field-test
  (testing "the synthetic subject PK field"
    (let [pk (build-pk-field)]
      (is (= "subject" (:name pk)))
      (is (true? (:pk? pk)))
      (is (zero? (:database-position pk))))))

(deftest build-field-from-uri-test
  (testing "field name is the shortened local name"
    (let [f (build-field-from-uri graph 0 (str graph "naam"))]
      (is (= "naam" (:name f)))
      (is (false? (:pk? f)))
      (is (= 1 (:database-position f))))))

(deftest shacl-prop->field-test
  (testing "an rdf:langString property gets the langString database-type"
    (let [f (shacl-prop->field graph false 0
                               {:property-uri (str graph "naam")
                                :base-type :type/Text
                                :lang-string? true
                                :description "Naam"})]
      (is (= "naam" (:name f)))
      (is (= "langString" (:database-type f)))
      (is (= "Naam" (:field-comment f)))))
  (testing "a required FK property carries semantic-type and database-required"
    (let [f (shacl-prop->field graph false 1
                               {:property-uri (str graph "geboorteplaats")
                                :base-type :type/Text
                                :semantic-type :type/FK
                                :database-required true})]
      (is (= :type/FK (:semantic-type f)))
      (is (true? (:database-required f)))))
  (testing "a foreign property is dropped when hide-foreign? is on"
    (is (nil? (shacl-prop->field graph true 0
                                 {:property-uri "http://xmlns.com/foaf/0.1/name"
                                  :base-type :type/Text})))))

(deftest shacl-shape->table-test
  (let [t (shacl-shape->table graph {:class-uri (str graph "Persoon")
                                     :description "A person"})]
    (is (= "Persoon" (:name t)))
    (is (= "Persoon" (:display-name t)))
    (is (= "A person" (:description t)))))

(deftest shacl-shape->describe-table-test
  (testing "properties are emitted PK-first and sorted by sh:order"
    (let [{:keys [name fields]}
          (shacl-shape->describe-table
           graph false
           {:class-uri (str graph "Persoon")
            :properties [{:property-uri (str graph "leeftijd") :base-type :type/Integer :order 2}
                         {:property-uri (str graph "naam") :base-type :type/Text :order 1}]})
          by-name (into {} (map (juxt :name identity)) fields)]
      (is (= "Persoon" name))
      (is (contains? by-name "subject"))
      (is (contains? by-name "naam"))
      (is (contains? by-name "leeftijd"))
      (is (true? (:pk? (by-name "subject"))))
      ;; sh:order 1 (naam) sorts before sh:order 2 (leeftijd); positions skip the PK.
      (is (< (:database-position (by-name "naam"))
             (:database-position (by-name "leeftijd")))))))

(deftest describe-database-none-test
  (testing "the 'none' sync strategy discovers no tables"
    (is (= {:tables #{}}
           (database/describe-database :sparql {:details {:metadata-sync-strategy "none"}})))))

(deftest describe-table-none-test
  (testing "the 'none' sync strategy returns an empty field set"
    (is (= {:name "Persoon" :schema nil :fields #{}}
           (database/describe-table :sparql
                                    {:details {:metadata-sync-strategy "none"}}
                                    {:name "Persoon"})))))

(deftest describe-database-explicit-test
  (testing "explicit JSON config drives table discovery without any endpoint I/O"
    (let [db {:name "ODIS"
              :details {:metadata-sync-strategy "explicit"
                        :default-graph graph
                        :schema-config (str "{\"tables\":[{\"name\":\"" graph "Persoon\","
                                            "\"fields\":[\"" graph "naam\"]}]}")}}
          {:keys [tables]} (database/describe-database :sparql db)]
      (is (= #{"Persoon"} (set (map :name tables)))))))

(deftest describe-table-explicit-test
  (testing "explicit config resolves a (shortened) table name back to its fields"
    (let [db {:name "ODIS"
              :details {:metadata-sync-strategy "explicit"
                        :default-graph graph
                        :schema-config (str "{\"tables\":[{\"name\":\"" graph "Persoon\","
                                            "\"fields\":[\"" graph "naam\"]}]}")}}
          {:keys [fields]} (database/describe-table :sparql db {:name "Persoon"})
          names (set (map :name fields))]
      (is (contains? names "subject"))
      (is (contains? names "naam")))))

(deftest fks-non-shacl-test
  (testing "fks returns nil for non-SHACL sync strategies"
    (is (nil? (database/fks {:details {:metadata-sync-strategy "auto"}})))
    (is (nil? (database/fks {:details {}})))))
