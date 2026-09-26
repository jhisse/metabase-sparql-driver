(ns metabase.driver.sparql.database-test
  "Unit tests for SPARQL metadata sync: the auto/explicit/none/shacl
   strategies (endpoint and SHACL fetch stubbed), foreign-URI hiding, and the
   SHACL shape -> Metabase metadata conversion. URI naming itself is covered in
   uri_test.clj."
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [metabase.driver.sparql.database :as database]
            [metabase.driver.sparql.execute :as execute]
            [metabase.driver.sparql.shacl :as shacl]))

(def ^:private extract-class-name @#'database/extract-class-name)
(def ^:private parse-schema-config @#'database/parse-schema-config)
(def ^:private build-pk-field @#'database/build-pk-field)
(def ^:private build-field-from-uri @#'database/build-field-from-uri)
(def ^:private shacl-prop->field @#'database/shacl-prop->field)
(def ^:private shacl-shape->table @#'database/shacl-shape->table)
(def ^:private shacl-shape->describe-table @#'database/shacl-shape->describe-table)

(def ^:private graph "https://example.org/")

(deftest extract-class-name-test
  (testing "local name is taken after the last slash or hash"
    (is (= "Persoon" (extract-class-name "https://example.org/Persoon")))
    (is (= "Person"  (extract-class-name "http://xmlns.com/foaf/0.1/Person")))
    (is (= "name"    (extract-class-name "http://example.org/schema#name")))))

(deftest parse-schema-config-test
  (testing "valid JSON is decoded with keyword keys"
    (is (= {:tables [{:name "https://example.org/Persoon"}]}
           (parse-schema-config "{\"tables\":[{\"name\":\"https://example.org/Persoon\"}]}"))))
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
      (is (= "string" (:database-type f)))
      (is (= 1 (:database-position f)))))
  (testing "an IRI-valued property gets the \"uri\" database-type marker"
    (is (= "uri" (:database-type (build-field-from-uri graph 0 (str graph "kent") true))))))

(deftest build-fields-from-sparql-query-iri-marker-test
  (let [build @#'database/build-fields-from-sparql-query
        row   (fn [prop is-iri]
                {:property {:type "uri" :value (str graph prop)}
                 :isIri    {:type "literal" :value is-iri}})
        fields (build graph false [(row "kent" "1") (row "naam" "0")])
        by-name (into {} (map (juxt :name identity)) fields)]
    (testing "?isIri = 1 marks the property as IRI-valued"
      (is (= "uri" (:database-type (by-name "kent")))))
    (testing "?isIri = 0 stays a plain string property"
      (is (= "string" (:database-type (by-name "naam")))))
    (testing "a row without ?isIri (the fallback query shape) defaults to string"
      (let [fields (build graph false [{:property {:type "uri" :value (str graph "los")}}])]
        (is (= "string" (:database-type (first (filter #(= "los" (:name %)) fields)))))))))

(deftest describe-table-auto-iri-projection-fallback-test
  (let [details   {:endpoint "http://sparql.invalid/query" :default-graph graph}
        queries   (atom [])
        one-prop  {:results {:bindings [{:property {:type "uri" :value (str graph "naam")}}]}}]
    (testing "an endpoint that rejects the ?isIri query still syncs its fields"
      (reset! queries [])
      (with-redefs [execute/execute-sparql-query
                    (fn [_ query _]
                      (swap! queries conj query)
                      (if (str/includes? query "?isIri")
                        [false "SPARQL endpoint returned status: 400" :query]
                        [true one-prop]))]
        (let [{:keys [fields]} (database/describe-table :sparql {:details details} {:name "Persoon"})
              naam (first (filter #(= "naam" (:name %)) fields))]
          (is (= 2 (count @queries)) "the rejected query is retried without the projection")
          (is (some? naam) "the property still syncs — no zero-field table")
          (is (= "string" (:database-type naam))
              "without the projection the IRI marker is simply absent"))))
    (testing "an endpoint/transport failure is NOT retried — a second round-trip would not help"
      (reset! queries [])
      (with-redefs [execute/execute-sparql-query
                    (fn [_ query _]
                      (swap! queries conj query)
                      [false "Connection refused" :transport])]
        (database/describe-table :sparql {:details details} {:name "Persoon"})
        (is (= 1 (count @queries)))))))

(deftest shacl-prop->field-test
  (testing "an IRI-node property (sh:nodeKind sh:IRI / sh:class) gets the uri database-type"
    (let [f (shacl-prop->field graph false 0
                               {:property-uri (str graph "website")
                                :base-type :type/Text
                                :iri-kind? true})]
      (is (= "uri" (:database-type f)))))
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
    (let [db {:name "example"
              :details {:metadata-sync-strategy "explicit"
                        :default-graph graph
                        :schema-config (str "{\"tables\":[{\"name\":\"" graph "Persoon\","
                                            "\"fields\":[\"" graph "naam\"]}]}")}}
          {:keys [tables]} (database/describe-database :sparql db)]
      (is (= #{"Persoon"} (set (map :name tables)))))))

(deftest describe-table-explicit-test
  (testing "explicit config resolves a (shortened) table name back to its fields"
    (let [db {:name "example"
              :details {:metadata-sync-strategy "explicit"
                        :default-graph graph
                        :schema-config (str "{\"tables\":[{\"name\":\"" graph "Persoon\","
                                            "\"fields\":[\"" graph "naam\"]}]}")}}
          {:keys [fields]} (database/describe-table :sparql db {:name "Persoon"})
          names (set (map :name fields))]
      (is (contains? names "subject"))
      (is (contains? names "naam")))))

(deftest fks-non-shacl-test
  (testing "fks returns an empty seq for non-SHACL sync strategies"
    (is (= [] (database/fks {:details {:metadata-sync-strategy "auto"}})))
    (is (= [] (database/fks {:details {}})))))

(deftest describe-table-explicit-with-prefixes-test
  (let [details {:metadata-sync-strategy "explicit"
                 :default-graph graph
                 :namespace-prefixes "foaf=http://xmlns.com/foaf/0.1/"
                 :schema-config (str "{\"tables\":[{\"name\":\"" graph "Persoon\","
                                     "\"fields\":[\"" graph "naam\","
                                     "\"http://xmlns.com/foaf/0.1/name\","
                                     "\"http://other.example/x\"]}]}")}]
    (testing "a prefix-namespace property syncs as prefix__localName"
      (let [{:keys [fields]} (database/describe-table :sparql {:details details} {:name "Persoon"})
            names (set (map :name fields))]
        (is (contains? names "naam"))
        (is (contains? names "foaf__name"))
        (is (contains? names "http://other.example/x"))))
    (testing "hide-foreign keeps prefix-namespace properties and drops the rest"
      (let [{:keys [fields]} (database/describe-table
                              :sparql
                              {:details (assoc details :hide-foreign-uris true)}
                              {:name "Persoon"})
            names (set (map :name fields))]
        (is (contains? names "foaf__name"))
        (is (not (contains? names "http://other.example/x")))))))

;; ---------------------------------------------------------------------------
;; auto strategy (class discovery against the endpoint)
;; ---------------------------------------------------------------------------

(defn- class-binding [class-uri n]
  {:class {:type "uri" :value class-uri} :count {:type "literal" :value (str n)}})

(deftest describe-database-auto-test
  (let [queries (atom [])
        details {:endpoint "http://sparql.invalid/query" :default-graph graph}]
    (with-redefs [execute/execute-sparql-query
                  (fn [_ query _]
                    (swap! queries conj query)
                    [true {:results {:bindings [(class-binding (str graph "Persoon") 12)
                                                (class-binding "http://xmlns.com/foaf/0.1/Agent" 3)]}}])]
      (testing "each discovered class becomes a table named by its shortened URI"
        (let [{:keys [tables]} (database/describe-database :sparql {:details details})
              by-name          (into {} (map (juxt :name identity)) tables)]
          (is (= #{"Persoon" "http://xmlns.com/foaf/0.1/Agent"} (set (keys by-name))))
          (is (= "Agent" (:display-name (by-name "http://xmlns.com/foaf/0.1/Agent"))))
          (is (str/includes? (:description (by-name "Persoon")) "Instances: 12"))))
      (testing "class-limit (a string in the manifest) bounds the discovery query; default is 100"
        (reset! queries [])
        (database/describe-database :sparql {:details (assoc details :class-limit " 5 ")})
        (database/describe-database :sparql {:details details})
        (is (str/ends-with? (first @queries) "LIMIT 5"))
        (is (str/ends-with? (second @queries) "LIMIT 100")))
      (testing "hide-foreign-uris drops classes outside the Default Graph"
        (is (= #{"Persoon"}
               (set (map :name (:tables (database/describe-database
                                         :sparql {:details (assoc details :hide-foreign-uris true)}))))))))
    (testing "an endpoint failure degrades to no tables instead of failing the sync"
      (with-redefs [execute/execute-sparql-query (fn [_ _ _] [false "boom" :db])]
        (is (= {:tables #{}} (database/describe-database :sparql {:details details})))))))

;; ---------------------------------------------------------------------------
;; shacl strategy (SHACL fetch stubbed at shacl/metadata)
;; ---------------------------------------------------------------------------

(def ^:private shapes
  [{:class-uri   (str graph "Persoon")
    :description "Een persoon"
    :properties  [{:property-uri (str graph "naam") :base-type :type/Text :order 1}
                  {:property-uri    (str graph "geboorteplaats")
                   :base-type       :type/Text
                   :semantic-type   :type/FK
                   :fk-target-class (str graph "Plaats")
                   :iri-kind?       true
                   :order           2}]}
   {:class-uri  (str graph "Plaats")
    :properties [{:property-uri (str graph "label") :base-type :type/Text}]}
   {:class-uri  "http://xmlns.com/foaf/0.1/Agent"
    :properties [{:property-uri (str graph "kent") :base-type :type/Text :fk-target-class (str graph "Persoon")}]}])

(defn- shacl-db [& {:as details}]
  {:name    "shacl"
   :details (merge {:metadata-sync-strategy "shacl"
                    :shacl-url              "https://example.org/shapes.ttl"
                    :default-graph          graph}
                   details)})

(deftest shacl-strategy-test
  (let [calls (atom [])]
    (with-redefs [shacl/metadata (fn [url lang opts] (swap! calls conj [url lang opts]) shapes)]
      (testing "describe-database: one table per shape"
        (is (= #{"Persoon" "Plaats" "http://xmlns.com/foaf/0.1/Agent"}
               (set (map :name (:tables (database/describe-database :sparql (shacl-db)))))))
        (is (= #{"Persoon" "Plaats"}
               (set (map :name (:tables (database/describe-database
                                         :sparql (shacl-db :hide-foreign-uris true))))))))
      (testing "describe-table: the matching shape's properties, PK first"
        (let [{:keys [fields]} (database/describe-table :sparql (shacl-db) {:name "Persoon"})
              by-name          (into {} (map (juxt :name identity)) fields)]
          (is (= #{"subject" "naam" "geboorteplaats"} (set (keys by-name))))
          (is (= "uri" (:database-type (by-name "geboorteplaats"))))
          (is (= :type/FK (:semantic-type (by-name "geboorteplaats"))))))
      (testing "describe-table: a table with no shape keeps only the synthetic PK"
        (is (= #{"subject"}
               (set (map :name (:fields (database/describe-table :sparql (shacl-db) {:name "Onbekend"})))))))
      (testing "fks: one row per sh:class property, pointing at the target's subject"
        (is (= #{{:fk-table-name "Persoon" :fk-table-schema nil :fk-column-name "geboorteplaats"
                  :pk-table-name "Plaats"  :pk-table-schema nil :pk-column-name "subject"}
                 {:fk-table-name "http://xmlns.com/foaf/0.1/Agent" :fk-table-schema nil :fk-column-name "kent"
                  :pk-table-name "Persoon" :pk-table-schema nil :pk-column-name "subject"}}
               (set (database/fks (shacl-db)))))
        (is (= ["Persoon"]
               (map :fk-table-name (database/fks (shacl-db :hide-foreign-uris true))))))
      (testing "connection settings reach the SHACL fetch: seconds -> ms, MB -> bytes, language"
        (reset! calls [])
        (database/describe-database :sparql (shacl-db :default-language "nl"
                                                      :shacl-connect-timeout "5"
                                                      :shacl-socket-timeout "60"
                                                      :shacl-max-size-mb "2"))
        (is (= [["https://example.org/shapes.ttl" "nl"
                 {:connect-timeout-ms 5000 :socket-timeout-ms 60000 :max-bytes 2097152}]]
               @calls))))
    (testing "a failed SHACL fetch degrades to an empty schema instead of failing the sync"
      (with-redefs [shacl/metadata (fn [& _] (throw (ex-info "Failed to fetch" {:status 500})))]
        (is (= {:tables #{}} (database/describe-database :sparql (shacl-db))))
        (is (empty? (database/fks (shacl-db))))
        (is (= #{"subject"}
               (set (map :name (:fields (database/describe-table :sparql (shacl-db) {:name "Persoon"}))))))))))
