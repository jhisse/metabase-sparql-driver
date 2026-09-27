(ns metabase.driver.sparql.shacl-test
  "Unit tests for SHACL-based metadata extraction: the pure parse/extract path
   (`parse-turtle` -> `shacl->metadata`), plus the HTTP fetch and the cache in
   `metadata` with the HTTP client stubbed."
  (:require [clj-http.client :as http]
            [clojure.test :refer :all]
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
   "ex:PersoonShape a sh:NodeShape ;\n"
   "  sh:targetClass ex:Persoon ;\n"
   "  sh:description \"Een persoon\"@nl ;\n"
   "  sh:node ex:EntiteitShape ;\n"
   "  sh:property ex:p_naam , ex:p_leeftijd , ex:p_geboorteplaats , ex:p_website , ex:p_secret .\n"
   "ex:p_naam a sh:PropertyShape ;\n"
   "  sh:path ex:naam ; sh:datatype rdf:langString ;\n"
   "  sh:name \"Naam\"@nl , \"Name\"@en ; sh:order 1 .\n"
   "ex:p_leeftijd a sh:PropertyShape ;\n"
   "  sh:path ex:leeftijd ; sh:datatype xsd:integer ; sh:minCount 1 ; sh:order 2 .\n"
   "ex:p_geboorteplaats a sh:PropertyShape ;\n"
   "  sh:path ex:geboorteplaats ; sh:class ex:Plaats ; sh:order 3 .\n"
   "ex:p_website a sh:PropertyShape ;\n"
   "  sh:path ex:website ; sh:nodeKind sh:IRI ; sh:order 4 .\n"
   "ex:p_secret a sh:PropertyShape ;\n"
   "  sh:path ex:secret ; sh:datatype xsd:string ; mb:hide true .\n"
   "\n"
   "ex:PlaatsShape a sh:NodeShape ;\n"
   "  sh:targetClass ex:Plaats ;\n"
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

(deftest malformed-min-count-test
  (testing "a non-integer sh:minCount is ignored instead of failing the whole document"
    (let [ttl    (str "@prefix sh: <http://www.w3.org/ns/shacl#> .\n"
                      "@prefix ex: <https://example.org/> .\n"
                      "ex:S a sh:NodeShape ; sh:targetClass ex:C ; sh:property ex:p1 , ex:p2 .\n"
                      "ex:p1 a sh:PropertyShape ; sh:path ex:a ; sh:minCount \"1.0\" .\n"
                      "ex:p2 a sh:PropertyShape ; sh:path ex:b ; sh:minCount \"one\" .\n")
          props  (-> (shacl->metadata (shacl/parse-turtle ttl base) "") first :properties)]
      (is (= 2 (count props)))
      (is (every? (comp false? :database-required) props)))))

(deftest shacl->metadata-test
  (let [shapes  (shacl->metadata (shacl/parse-turtle turtle base) "nl")
        by-cls  (into {} (map (juxt :class-uri identity)) shapes)
        persoon (by-cls (str base "Persoon"))
        plaats  (by-cls (str base "Plaats"))
        p-props (props-by-uri persoon)]

    (testing "every class-targeted shape becomes a table"
      (is (= 3 (count shapes)))
      (is (contains? by-cls (str base "Persoon")))
      (is (contains? by-cls (str base "Plaats")))
      (is (contains? by-cls (str base "Entiteit"))))

    (testing "the localized sh:description is selected"
      (is (= "Een persoon" (:description persoon)))
      (is (false? (:hidden? persoon))))

    (testing "metabase:hide properties are pruned"
      (is (not (contains? p-props (str base "secret")))))

    (testing "sh:node inheritance flattens parent properties into the child"
      (is (contains? p-props (str base "bron"))))

    (testing "rdf:langString properties are flagged and typed as text"
      (let [naam (p-props (str base "naam"))]
        (is (true? (:lang-string? naam)))
        (is (= :type/Text (:base-type naam)))
        (is (= "Naam" (:description naam)))))

    (testing "xsd:integer + sh:minCount are mapped"
      (let [leeftijd (p-props (str base "leeftijd"))]
        (is (= :type/Integer (:base-type leeftijd)))
        (is (true? (:database-required leeftijd)))))

    (testing "sh:class declares a foreign key"
      (let [gp (p-props (str base "geboorteplaats"))]
        (is (= :type/FK (:semantic-type gp)))
        (is (= (str base "Plaats") (:fk-target-class gp)))))

    (testing "IRI-node properties are flagged: sh:class targets and sh:nodeKind sh:IRI"
      (is (true? (:iri-kind? (p-props (str base "geboorteplaats")))))
      (is (true? (:iri-kind? (p-props (str base "website")))))
      (is (false? (:iri-kind? (p-props (str base "naam"))))))

    (testing "a leaf shape keeps just its own property"
      (is (= #{(str base "label")}
             (set (map :property-uri (:properties plaats))))))))

(deftest shacl->metadata-blank-node-shape-test
  (testing "a NodeShape written as a blank node keeps its own and inherited properties"
    (let [ttl    (str "@prefix sh:  <http://www.w3.org/ns/shacl#> .\n"
                      "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
                      "@prefix ex:  <https://example.org/> .\n"
                      "ex:EntiteitShape a sh:NodeShape ; sh:targetClass ex:Entiteit ;\n"
                      "  sh:property [ sh:path ex:bron ; sh:datatype xsd:string ] .\n"
                      "[] a sh:NodeShape ; sh:targetClass ex:Boek ; sh:node ex:EntiteitShape ;\n"
                      "  sh:property [ sh:path ex:titel ; sh:datatype xsd:string ] .\n")
          boek   (->> (shacl->metadata (shacl/parse-turtle ttl base) "nl")
                      (filter #(= (str base "Boek") (:class-uri %)))
                      first)]
      (is (= #{(str base "titel") (str base "bron")}
             (set (keys (props-by-uri boek))))))))

(deftest shacl->metadata-language-test
  (testing "switching the language re-picks sh:name literals"
    (let [shapes (shacl->metadata (shacl/parse-turtle turtle base) "en")
          naam   ((props-by-uri (first (filter #(= (str base "Persoon") (:class-uri %)) shapes)))
                  (str base "naam"))]
      (is (= "Name" (:description naam))))))

(deftest pick-localized-test
  (let [nl {:type :literal :value "Naam" :lang "nl"}
        en {:type :literal :value "Name" :lang "en"}
        un {:type :literal :value "Plain" :lang nil}]
    (is (= "Name" (pick-localized [nl en] "en")))
    (is (= "Naam" (pick-localized [nl en] "nl")))
    (testing "untagged literal is preferred when no language matches"
      (is (= "Plain" (pick-localized [nl un] "fr"))))
    (testing "falls back to the first literal when nothing else fits"
      (is (= "Naam" (pick-localized [nl en] ""))))
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

(deftest fetch-shacl-test
  (let [req (atom nil)
        respond-with (fn [resp] (fn [url opts] (reset! req {:url url :opts opts}) resp))]
    (testing "a 200 returns the body and asks for Turtle with the default limits"
      (with-redefs [http/get (respond-with {:status 200 :body "ttl"})]
        (is (= "ttl" (shacl/fetch-shacl "https://example.org/shapes.ttl")))
        (is (= "text/turtle" (get-in @req [:opts :headers "Accept"])))
        (is (= 10000 (get-in @req [:opts :connection-timeout])))
        (is (= 30000 (get-in @req [:opts :socket-timeout])))))
    (testing "configured timeouts are forwarded"
      (with-redefs [http/get (respond-with {:status 200 :body "ttl"})]
        (shacl/fetch-shacl "https://example.org/shapes.ttl" {:connect-timeout-ms 1 :socket-timeout-ms 2})
        (is (= [1 2] ((juxt :connection-timeout :socket-timeout) (:opts @req))))))
    (testing "a non-200 throws with the status, instead of parsing an error page"
      (with-redefs [http/get (respond-with {:status 404 :body "not found"})]
        (let [e (is (thrown? clojure.lang.ExceptionInfo (shacl/fetch-shacl "https://example.org/x.ttl")))]
          (is (= 404 (:status (ex-data e)))))))
    (testing "credentials in the URL stay out of the error"
      (with-redefs [http/get (respond-with {:status 404 :body "not found"})]
        (let [e (is (thrown? clojure.lang.ExceptionInfo (shacl/fetch-shacl "https://u:secret@example.org/x.ttl")))]
          (is (not (re-find #"secret" (ex-message e))))
          (is (= "https://example.org/x.ttl" (:url (ex-data e)))))))
    (testing "a body over the size cap is rejected (counted in UTF-8 bytes, not chars)"
      (with-redefs [http/get (respond-with {:status 200 :body "ééé"})]
        (is (= "ééé" (shacl/fetch-shacl "https://example.org/x.ttl" {:max-bytes 6})))
        (let [e (is (thrown? clojure.lang.ExceptionInfo
                             (shacl/fetch-shacl "https://example.org/x.ttl" {:max-bytes 5})))]
          (is (= 6 (:bytes (ex-data e)))))))))

(deftest metadata-cache-test
  (let [url     "https://example.org/cache-test.ttl"
        fetches (atom 0)]
    (with-redefs [shacl/fetch-shacl (fn [_ _] (swap! fetches inc) turtle)]
      (shacl/invalidate! url)
      (testing "the first call fetches, a repeat within the TTL is served from cache"
        (is (= 3 (count (shacl/metadata url "nl"))))
        (shacl/metadata url "nl")
        (is (= 1 @fetches)))
      (testing "labels are resolved per language, so a new language is a cache miss"
        (shacl/metadata url "en")
        (is (= 2 @fetches)))
      (testing "an entry older than the TTL is refetched"
        (swap! @#'shacl/cache assoc-in [[url "nl" nil] :at] 0)
        (shacl/metadata url "nl")
        (is (= 3 @fetches)))
      (testing "different fetch options are a cache miss, so each size cap is applied"
        (shacl/metadata url "nl" {:max-bytes 100})
        (shacl/metadata url "nl" {:max-bytes 100})
        (is (= 4 @fetches)))
      (testing "invalidate! drops every language and option set for the URL"
        (shacl/invalidate! url)
        (shacl/metadata url "nl")
        (shacl/metadata url "en")
        (shacl/metadata url "nl" {:max-bytes 100})
        (is (= 7 @fetches)))
      (shacl/invalidate! url))))
