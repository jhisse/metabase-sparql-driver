(ns metabase.driver.sparql.shacl-test
  "Unit tests for SHACL-based metadata extraction. These exercise the pure
   parse/extract path (`parse-turtle` -> `shacl->metadata`); the HTTP fetch and
   caching in `metadata` are not covered here."
  (:require [clojure.test :refer :all]
            [metabase.driver.sparql.shacl :as shacl]))

(def ^:private shacl->metadata @#'shacl/shacl->metadata)
(def ^:private pick-localized @#'shacl/pick-localized)
(def ^:private coerce-semantic-type @#'shacl/coerce-semantic-type)
(def ^:private xsd-base-type @#'shacl/xsd-base-type)

(def ^:private base "https://example.org/")

(def ^:private turtle
  (str
   "@prefix sh:  <http://www.w3.org/ns/shacl#> .\n"
   "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
   "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
   "@prefix mb:  <https://data.metabase.com/> .\n"
   "@prefix ex:  <https://example.org/> .\n"
   "\n"
   "ex:EntiteitShape a sh:NodeShape ;\n"
   "  sh:targetClass ex:Entiteit ;\n"
   "  sh:property ex:p_bron .\n"
   "ex:p_bron a sh:PropertyShape ; sh:path ex:bron ; sh:datatype xsd:string .\n"
   "\n"
   "ex:PersonShape a sh:NodeShape ;\n"
   "  sh:targetClass ex:Person ;\n"
   "  sh:description \"Een person\"@nl ;\n"
   "  sh:node ex:EntiteitShape ;\n"
   "  sh:property ex:p_name , ex:p_age , ex:p_birthplace , ex:p_secret .\n"
   "ex:p_name a sh:PropertyShape ;\n"
   "  sh:path ex:name ; sh:datatype rdf:langString ;\n"
   "  sh:name \"Name\"@nl , \"Name\"@en ; sh:order 1 .\n"
   "ex:p_age a sh:PropertyShape ;\n"
   "  sh:path ex:age ; sh:datatype xsd:integer ; sh:minCount 1 ; sh:order 2 .\n"
   "ex:p_birthplace a sh:PropertyShape ;\n"
   "  sh:path ex:birthplace ; sh:class ex:Place ; sh:order 3 .\n"
   "ex:p_secret a sh:PropertyShape ;\n"
   "  sh:path ex:secret ; sh:datatype xsd:string ; mb:hide true .\n"
   "\n"
   "ex:PlaceShape a sh:NodeShape ;\n"
   "  sh:targetClass ex:Place ;\n"
   "  sh:property ex:p_label .\n"
   "ex:p_label a sh:PropertyShape ; sh:path ex:label ; sh:datatype xsd:string .\n"))

(defn- props-by-uri [shape]
  (into {} (map (juxt :property-uri identity)) (:properties shape)))

(deftest parse-turtle-test
  (let [triples (shacl/parse-turtle turtle base)]
    (testing "parsing yields a non-empty vector of [s p o] triples"
      (is (vector? triples))
      (is (pos? (count triples)))
      (is (every? #(= 3 (count %)) triples)))
    (testing "three sh:NodeShape declarations are present"
      (let [node-shape-triples (filter (fn [[_ p o]]
                                         (and (= "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                                                 (:value p))
                                              (= "http://www.w3.org/ns/shacl#NodeShape"
                                                 (:value o))))
                                       triples)]
        (is (= 3 (count node-shape-triples)))))))

(deftest shacl->metadata-test
  (let [shapes  (shacl->metadata (shacl/parse-turtle turtle base) "nl")
        by-cls  (into {} (map (juxt :class-uri identity)) shapes)
        person (by-cls (str base "Person"))
        place  (by-cls (str base "Place"))
        p-props (props-by-uri person)]

    (testing "every class-targeted shape becomes a table"
      (is (= 3 (count shapes)))
      (is (contains? by-cls (str base "Person")))
      (is (contains? by-cls (str base "Place")))
      (is (contains? by-cls (str base "Entiteit"))))

    (testing "the localized sh:description is selected"
      (is (= "Een person" (:description person)))
      (is (false? (:hidden? person))))

    (testing "metabase:hide properties are pruned"
      (is (not (contains? p-props (str base "secret")))))

    (testing "sh:node inheritance flattens parent properties into the child"
      (is (contains? p-props (str base "bron"))))

    (testing "rdf:langString properties are flagged and typed as text"
      (let [name (p-props (str base "name"))]
        (is (true? (:lang-string? name)))
        (is (= :type/Text (:base-type name)))
        (is (= "Name" (:description name)))))

    (testing "xsd:integer + sh:minCount are mapped"
      (let [age (p-props (str base "age"))]
        (is (= :type/Integer (:base-type age)))
        (is (true? (:database-required age)))))

    (testing "sh:class declares a foreign key"
      (let [gp (p-props (str base "birthplace"))]
        (is (= :type/FK (:semantic-type gp)))
        (is (= (str base "Place") (:fk-target-class gp)))))

    (testing "a leaf shape keeps just its own property"
      (is (= #{(str base "label")}
             (set (map :property-uri (:properties place))))))))

(deftest shacl->metadata-language-test
  (testing "switching the language re-picks sh:name literals"
    (let [shapes (shacl->metadata (shacl/parse-turtle turtle base) "en")
          name   ((props-by-uri (first (filter #(= (str base "Person") (:class-uri %)) shapes)))
                  (str base "name"))]
      (is (= "Name" (:description name))))))

(deftest pick-localized-test
  (let [nl {:type :literal :value "Name" :lang "nl"}
        en {:type :literal :value "Name" :lang "en"}
        un {:type :literal :value "Plain" :lang nil}]
    (is (= "Name" (pick-localized [nl en] "en")))
    (is (= "Name" (pick-localized [nl en] "nl")))
    (testing "untagged literal is preferred when no language matches"
      (is (= "Plain" (pick-localized [nl un] "fr"))))
    (testing "falls back to the first literal when nothing else fits"
      (is (= "Name" (pick-localized [nl en] ""))))
    (is (nil? (pick-localized [] "nl")))))

(deftest coerce-semantic-type-test
  (is (= :type/URL (coerce-semantic-type {:type :literal :value "type/URL"})))
  (is (= :type/FK  (coerce-semantic-type {:type :literal :value ":type/FK"})))
  (is (nil? (coerce-semantic-type {:type :literal :value ""})))
  (is (nil? (coerce-semantic-type nil))))

(deftest xsd-base-type-test
  (is (= :type/Integer        (xsd-base-type "http://www.w3.org/2001/XMLSchema#integer")))
  (is (= :type/Float          (xsd-base-type "http://www.w3.org/2001/XMLSchema#decimal")))
  (is (= :type/DateTimeWithTZ (xsd-base-type "http://www.w3.org/2001/XMLSchema#dateTime")))
  (is (= :type/Date           (xsd-base-type "http://www.w3.org/2001/XMLSchema#date")))
  (testing "non-XSD datatypes are not resolved here"
    (is (nil? (xsd-base-type "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")))))
