(ns metabase.driver.sparql.database-test
  "Unit tests for SPARQL metadata-sync helpers: URI shortening, the implicit
   Default-Graph base prefix, foreign-URI hiding, explicit/none sync strategies,
   and the SHACL shape -> Metabase metadata conversion."
  (:require [clojure.test :refer :all]
            [metabase.driver.sparql.database :as database]
            [metabase.driver.sparql.uri :as uri]))

(def ^:private extract-class-name @#'database/extract-class-name)
(def ^:private shorten-uri uri/shorten-uri)
(def ^:private foreign-uri? uri/foreign-uri?)
(def ^:private absolute-uri uri/absolute-uri)
(def ^:private parse-schema-config @#'database/parse-schema-config)
(def ^:private build-pk-field @#'database/build-pk-field)
(def ^:private build-field-from-uri @#'database/build-field-from-uri)
(def ^:private shacl-prop->field @#'database/shacl-prop->field)
(def ^:private shacl-shape->table @#'database/shacl-shape->table)
(def ^:private shacl-shape->describe-table @#'database/shacl-shape->describe-table)

(def ^:private graph "https://example.org/")

(deftest extract-class-name-test
  (testing "local name is taken after the last slash or hash"
    (is (= "Person" (extract-class-name "https://example.org/Person")))
    (is (= "Person"  (extract-class-name "http://xmlns.com/foaf/0.1/Person")))
    (is (= "name"    (extract-class-name "http://example.org/schema#name")))))

(deftest shorten-uri-test
  (testing "the Default-Graph prefix is stripped"
    (is (= "Person" (shorten-uri (str graph "Person") graph))))
  (testing "URIs outside the Default Graph are left untouched"
    (is (= "http://xmlns.com/foaf/0.1/Person"
           (shorten-uri "http://xmlns.com/foaf/0.1/Person" graph))))
  (testing "a blank Default Graph is a no-op"
    (is (= (str graph "Person") (shorten-uri (str graph "Person") ""))))
  (testing "stripping never yields a blank name (uri == default-graph)"
    (is (= graph (shorten-uri graph graph)))))

(deftest foreign-uri?-test
  (is (true?  (foreign-uri? "http://xmlns.com/foaf/0.1/Person" graph)))
  (is (false? (foreign-uri? (str graph "Person") graph)))
  (testing "without a Default Graph nothing is considered foreign"
    (is (false? (foreign-uri? "http://xmlns.com/foaf/0.1/Person" "")))))

(deftest absolute-uri-test
  (testing "relative names are resolved against the Default Graph"
    (is (= (str graph "name") (absolute-uri "name" graph))))
  (testing "already-absolute URIs are returned unchanged"
    (is (= "http://xmlns.com/foaf/0.1/name" (absolute-uri "http://xmlns.com/foaf/0.1/name" graph))))
  (testing "blank inputs are no-ops"
    (is (= "" (absolute-uri "" graph)))
    (is (= "name" (absolute-uri "name" "")))))

(deftest parse-schema-config-test
  (testing "valid JSON is decoded with keyword keys"
    (is (= {:tables [{:name "https://example.org/Person"}]}
           (parse-schema-config "{\"tables\":[{\"name\":\"https://example.org/Person\"}]}"))))
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
    (let [f (build-field-from-uri graph 0 (str graph "name"))]
      (is (= "name" (:name f)))
      (is (false? (:pk? f)))
      (is (= 1 (:database-position f))))))

(deftest shacl-prop->field-test
  (testing "an rdf:langString property gets the langString database-type"
    (let [f (shacl-prop->field graph false 0
                               {:property-uri (str graph "name")
                                :base-type :type/Text
                                :lang-string? true
                                :description "Name"})]
      (is (= "name" (:name f)))
      (is (= "langString" (:database-type f)))
      (is (= "Name" (:field-comment f)))))
  (testing "a required FK property carries semantic-type and database-required"
    (let [f (shacl-prop->field graph false 1
                               {:property-uri (str graph "birthplace")
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
  (let [t (shacl-shape->table graph {:class-uri (str graph "Person")
                                     :description "A person"})]
    (is (= "Person" (:name t)))
    (is (= "Person" (:display-name t)))
    (is (= "A person" (:description t)))))

(deftest shacl-shape->describe-table-test
  (testing "properties are emitted PK-first and sorted by sh:order"
    (let [{:keys [name fields]}
          (shacl-shape->describe-table
           graph false
           {:class-uri (str graph "Person")
            :properties [{:property-uri (str graph "age") :base-type :type/Integer :order 2}
                         {:property-uri (str graph "name") :base-type :type/Text :order 1}]})
          by-name (into {} (map (juxt :name identity)) fields)]
      (is (= "Person" name))
      (is (contains? by-name "subject"))
      (is (contains? by-name "name"))
      (is (contains? by-name "age"))
      (is (true? (:pk? (by-name "subject"))))
      ;; sh:order 1 (name) sorts before sh:order 2 (age); positions skip the PK.
      (is (< (:database-position (by-name "name"))
             (:database-position (by-name "age")))))))

(deftest describe-database-none-test
  (testing "the 'none' sync strategy discovers no tables"
    (is (= {:tables #{}}
           (database/describe-database :sparql {:details {:metadata-sync-strategy "none"}})))))

(deftest describe-table-none-test
  (testing "the 'none' sync strategy returns an empty field set"
    (is (= {:name "Person" :schema nil :fields #{}}
           (database/describe-table :sparql
                                    {:details {:metadata-sync-strategy "none"}}
                                    {:name "Person"})))))

(deftest describe-database-explicit-test
  (testing "explicit JSON config drives table discovery without any endpoint I/O"
    (let [db {:name "Example"
              :details {:metadata-sync-strategy "explicit"
                        :default-graph graph
                        :schema-config (str "{\"tables\":[{\"name\":\"" graph "Person\","
                                            "\"fields\":[\"" graph "name\"]}]}")}}
          {:keys [tables]} (database/describe-database :sparql db)]
      (is (= #{"Person"} (set (map :name tables)))))))

(deftest describe-table-explicit-test
  (testing "explicit config resolves a (shortened) table name back to its fields"
    (let [db {:name "Example"
              :details {:metadata-sync-strategy "explicit"
                        :default-graph graph
                        :schema-config (str "{\"tables\":[{\"name\":\"" graph "Person\","
                                            "\"fields\":[\"" graph "name\"]}]}")}}
          {:keys [fields]} (database/describe-table :sparql db {:name "Person"})
          names (set (map :name fields))]
      (is (contains? names "subject"))
      (is (contains? names "name")))))

(deftest fks-non-shacl-test
  (testing "fks returns an empty seq for non-SHACL sync strategies"
    (is (= [] (database/fks {:details {:metadata-sync-strategy "auto"}})))
    (is (= [] (database/fks {:details {}})))))

(def ^:private build-fields-from-sparql-query @#'database/build-fields-from-sparql-query)
(def ^:private coordinate-fields @#'database/coordinate-fields)

(deftest build-field-from-uri-geometry-test
  (testing "a WKT-shaped sample marks the column as geometry"
    (is (= "geometry" (:database-type (build-field-from-uri graph 0 (str graph "location") "POINT(4.7 50.8)"))))
    (is (= "geometry" (:database-type (build-field-from-uri graph 0 (str graph "vak") "BOX(1 2,3 4)"))))
    (is (= "string"   (:database-type (build-field-from-uri graph 0 (str graph "name") "Antwerpen"))))))

(deftest coordinate-fields-test
  (testing "POINT source → lon/lat typed Longitude/Latitude"
    (let [fs (coordinate-fields [["location" :point]] 5)
          by-name (into {} (map (juxt :name identity)) fs)]
      (is (= #{"location_lon" "location_lat"} (set (keys by-name))))
      (is (= :type/Longitude (:semantic-type (by-name "location_lon"))))
      (is (= :type/Latitude  (:semantic-type (by-name "location_lat"))))
      (is (= "geo-coord:point-lon:location" (:database-type (by-name "location_lon"))))
      (is (every? #(= :type/Float (:base-type %)) fs))))
  (testing "BOX source → four corner columns typed Coordinate"
    (let [fs (coordinate-fields [["vak" :box]] 0)]
      (is (= ["vak_min_lon" "vak_max_lon" "vak_min_lat" "vak_max_lat"] (map :name fs)))
      (is (every? #(= :type/Coordinate (:semantic-type %)) fs))
      (is (= "geo-coord:box-max-lat:vak" (:database-type (last fs))))))
  (testing "polygon / non-extractable kinds yield nothing"
    (is (empty? (coordinate-fields [["geom" :polygon]] 0)))))

(deftest build-fields-from-sparql-query-geometry-test
  (testing "the sampling path synthesizes lon/lat for a POINT geometry property"
    (let [bindings [{:property {:value (str graph "name")}    :sample {:value "Antwerpen"}}
                    {:property {:value (str graph "location")} :sample {:value "POINT(4.7 50.8)"}}]
          fields   (build-fields-from-sparql-query graph false bindings)
          by-name  (into {} (map (juxt :name identity)) fields)]
      (is (contains? by-name "location"))
      (is (= "geometry" (:database-type (by-name "location"))))
      (is (contains? by-name "location_lon"))
      (is (contains? by-name "location_lat"))
      (is (= :type/Longitude (:semantic-type (by-name "location_lon"))))
      (is (not (contains? by-name "name_lon"))))))

(deftest shacl-describe-table-geometry-test
  (testing "the SHACL path synthesizes point lon/lat for a geometry property"
    (let [{:keys [fields]}
          (shacl-shape->describe-table
           graph false
           {:class-uri (str graph "Place")
            :properties [{:property-uri (str graph "location") :base-type :type/Text :geometry? true :order 1}]})
          by-name (into {} (map (juxt :name identity)) fields)]
      (is (= "geometry" (:database-type (by-name "location"))))
      (is (= :type/Longitude (:semantic-type (by-name "location_lon"))))
      (is (= :type/Latitude  (:semantic-type (by-name "location_lat")))))))
