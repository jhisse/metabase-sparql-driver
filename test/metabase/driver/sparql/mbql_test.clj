(ns metabase.driver.sparql.mbql-test
  "Unit tests for the MBQL -> SPARQL transpiler.

   Pure helpers are tested directly. The stage-compilation functions reach the
   metadata provider through four private accessors
   (`field-id->metadata`, `table-id->class-uri`, `database-default-graph`,
   `database-default-language`); those are stubbed with `with-redefs-fn` so the
   full compile path can be exercised without a running Metabase app DB."
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [metabase.driver.sparql.mbql :as mbql]))

;; ---------------------------------------------------------------------------
;; Pure helpers
;; ---------------------------------------------------------------------------

(deftest sanitize-var-name-test
  (let [f @#'mbql/sanitize-var-name]
    (is (= "name" (f "name")))
    (is (= "birth_place" (f "birth-place")))
    (is (= "a_b_c" (f "a.b/c")))
    (testing "a leading digit is escaped"
      (is (= "_1col" (f "1col"))))
    (testing "blank input yields a usable default"
      (is (= "v" (f ""))))))

(deftest field-token-accessors-test
  (let [id    @#'mbql/field-token->id
        opts  @#'mbql/field-token->opts
        alias @#'mbql/field-token->join-alias]
    (is (= 5 (id [:field 5 {:join-alias "J"}])))
    (is (= "name" (id [:field "name" nil])))
    (is (nil? (id [:not-a-field 5])))
    (is (= {:join-alias "J"} (opts [:field 5 {:join-alias "J"}])))
    (is (nil? (opts [:field 5 nil])))
    (is (= "J" (alias [:field 5 {:join-alias "J"}])))
    (is (nil? (alias [:field 5 nil])))))

(deftest literal->sparql-test
  (let [f @#'mbql/literal->sparql]
    (is (= "\"Alice\"" (f "Alice")))
    (is (= "25" (f 25)))
    (is (= "true" (f true)))
    (is (= "false" (f false)))
    (is (= "" (f nil)))
    (testing "embedded double quotes are escaped"
      (is (= "\"a\\\"b\"" (f "a\"b"))))))

(deftest condition->fk-ref-test
  (let [fk-ref @#'mbql/condition->fk-ref
        fk-id  @#'mbql/condition->fk-field-id]
    (testing "the non-join-alias side of an = is the FK ref"
      (is (= [:field 1 nil]
             (fk-ref [:= [:field 1 nil] [:field 2 {:join-alias "J"}]])))
      (is (= 1 (fk-id [:= [:field 1 nil] [:field 2 {:join-alias "J"}]]))))
    (testing "an :and wrapper is unwrapped to its first ="
      (is (= [:field 1 nil]
             (fk-ref [:and [:= [:field 1 nil] [:field 2 {:join-alias "J"}]]]))))))

(deftest collect-field-ids-test
  (let [f @#'mbql/collect-field-ids]
    (is (= #{1 2 3 4}
           (set (f {:fields   [[:field 1 nil] [:field 2 nil]]
                    :order-by [[:asc [:field 3 nil]]]
                    :filter   [:= [:field 4 nil] 5]}))))))

(deftest collect-joined-pairs-test
  (let [f @#'mbql/collect-joined-pairs]
    (is (= #{[2 "J"]}
           (f {:fields [[:field 1 nil] [:field 2 {:join-alias "J"}]]})))))

(deftest aggregation-helpers-test
  (let [unwrap  @#'mbql/unwrap-aggregation
        arg-tok @#'mbql/aggregation-arg-token]
    (is (= [:count] (unwrap [:aggregation-options [:count] {:name "c"}])))
    (is (= [:count] (unwrap [:count])))
    (is (= [:field 5 nil] (arg-tok [:sum [:field 5 nil]])))
    (is (nil? (arg-tok [:count])))))

(deftest aggregation->projection-test
  (let [f @#'mbql/aggregation->projection]
    (testing "arg-less count is a DISTINCT subject count in a base stage"
      (is (= {:select "(COUNT(DISTINCT ?subject) AS ?ag_0)" :var "ag_0"}
             (f [:count] 0 (constantly nil)))))
    (testing "arg-less count becomes COUNT(*) when count-all? is set"
      (is (= {:select "(COUNT(*) AS ?ag_0)" :var "ag_0"}
             (f [:count] 0 (constantly nil) true))))
    (testing "sum/avg/min/max/distinct projections"
      (is (= {:select "(SUM(?amount) AS ?ag_1)" :var "ag_1"}
             (f [:sum [:field "amount" nil]] 1 (constantly "amount"))))
      (is (= {:select "(MIN(?amount) AS ?ag_0)" :var "ag_0"}
             (f [:min [:field "amount" nil]] 0 (constantly "amount"))))
      (is (= {:select "(COUNT(DISTINCT ?amount) AS ?ag_0)" :var "ag_0"}
             (f [:distinct [:field "amount" nil]] 0 (constantly "amount")))))
    (testing "an :aggregation-options wrapper is transparent"
      (is (= {:select "(COUNT(*) AS ?ag_0)" :var "ag_0"}
             (f [:aggregation-options [:count] {:name "c"}] 0 (constantly nil) true))))
    (testing "unsupported aggregations compile to nil"
      (is (nil? (f [:stddev [:field "amount" nil]] 0 (constantly "amount")))))))

(deftest compile-filter-expr-test
  (let [f #(@#'mbql/compile-filter-expr % {"name" "name" "age" "age"} {})]
    (is (= "(?name = \"John\")"        (f [:= [:field "name" nil] "John"])))
    (testing "a wrapped [:value ...] rhs is unwrapped"
      (is (= "(?name = \"John\")"      (f [:= [:field "name" nil] [:value "John" {}]]))))
    (is (= "(!BOUND(?name))"          (f [:= [:field "name" nil] nil])))
    (is (= "(BOUND(?name))"           (f [:!= [:field "name" nil] nil])))
    (is (= "(?age > 18)"         (f [:> [:field "age" nil] 18])))
    (is (= "(?name != \"John\")"       (f [:!= [:field "name" nil] "John"])))
    (testing "case-insensitive contains"
      (is (= "(CONTAINS(LCASE(STR(?name)), LCASE(\"an\")))"
             (f [:contains [:field "name" nil] "an" {:case-sensitive false}]))))
    (testing "boolean combinators"
      (is (= "((?name = \"John\") && (?age > 18))"
             (f [:and [:= [:field "name" nil] "John"] [:> [:field "age" nil] 18]])))
      (is (= "((?name = \"John\") || (?name = \"Pete\"))"
             (f [:or [:= [:field "name" nil] "John"] [:= [:field "name" nil] "Pete"]]))))
    (testing "IRI-valued fields render a URL value as <IRI>, not a string literal"
      (with-redefs [mbql/field-id->metadata {5 {:name "country" :semantic-type :type/FK}
                                             6 {:name "homepage" :semantic-type :type/URL}
                                             2 {:name "name" :database-type "string"}}]
        (let [g #(@#'mbql/compile-filter-expr % {5 "country" 6 "homepage" 2 "name"} {})
              iri "https://example.org/countries/AC28-7090"]
          (is (= (str "(?country = <" iri ">)")
                 (g [:= [:field 5 nil] iri]))
              "FK field + URL value → IRI term")
          (is (= (str "(?homepage != <" iri ">)")
                 (g [:!= [:field 6 nil] iri]))
              "URL field + URL value → IRI term")
          (is (= "(?country = \"AC-123\")"
                 (g [:= [:field 5 nil] "AC-123"]))
              "FK field + non-URL value stays a string literal")
          (is (= (str "(?name = \"" iri "\")")
                 (g [:= [:field 2 nil] iri]))
              "plain string field + URL-shaped value stays a string literal"))))))

(deftest order-by-test
  (let [ob     #(@#'mbql/compile-order-by % {"name" "name" "age" "age"} {})
        agg-ob #(@#'mbql/compile-agg-order-by % {"name" "name"} {})]
    (is (= "ORDER BY ASC(?name)" (ob [[:asc [:field "name" nil]]])))
    (is (= "ORDER BY DESC(?name) ASC(?age)"
           (ob [[:desc [:field "name" nil]] [:asc [:field "age" nil]]])))
    (is (nil? (ob [])))
    (testing "aggregation order-by can reference an aggregation by index"
      (is (= "ORDER BY DESC(?ag_0)" (agg-ob [[:desc [:aggregation 0]]])))
      (is (= "ORDER BY ASC(?name)"  (agg-ob [[:asc [:field "name" nil]]]))))))

(deftest var-for-token-test
  (let [f @#'mbql/var-for-token]
    (is (= "name" (f [:field "name" nil] {"name" "name"} {})))
    (testing "a join-alias token resolves through pair->target-var"
      (is (= "jvar" (f [:field "x" {:join-alias "J"}] {} {["x" "J"] "jvar"}))))))

(deftest inner-var-for-ref-test
  (let [f @#'mbql/inner-var-for-ref]
    (testing "a source-query column (string id) resolves to its sanitized name"
      (is (= "ag_0" (f [:field "ag_0" nil])))
      (is (= "birth_place" (f [:field "birth-place" nil]))))))

;; ---------------------------------------------------------------------------
;; Stage compilation (metadata accessors stubbed)
;; ---------------------------------------------------------------------------

(def ^:private base "https://example.org/")

(def ^:private fixture-fields
  {1  {:name "subject"}
   2  {:name "name" :database-type "string"}
   3  {:name "age" :database-type "string"}
   4  {:name "birthplace" :database-type "string"}
   5  {:name "country" :database-type "string"
       :semantic-type :type/FK :fk-target-class (str base "Country")}
   6  {:name "homepage" :database-type "string" :base-type :type/URL :semantic-type :type/URL}
   10 {:name "label" :database-type "string"}
   11 {:name "location" :database-type "geometry"}
   12 {:name "location_lon" :database-type "geo-coord:point-lon:location"
       :base-type :type/Float :semantic-type :type/Longitude}
   13 {:name "location_lat" :database-type "geo-coord:point-lat:location"
       :base-type :type/Float :semantic-type :type/Latitude}
   14 {:name "bbox_min_lon" :database-type "geo-coord:box-min-lon:bbox"
       :base-type :type/Float :semantic-type :type/Coordinate}
   15 {:name "bbox_max_lat" :database-type "geo-coord:box-max-lat:bbox"
       :base-type :type/Float :semantic-type :type/Coordinate}})

(defn- compile-stage* [stage]
  (@#'mbql/compile-stage stage))

(defmacro ^:private with-fixture
  "Run `body` with the four metadata accessors stubbed for the Example fixture."
  [& body]
  `(with-redefs-fn
     {#'mbql/field-id->metadata        (fn [id#] (get fixture-fields id#))
      #'mbql/table-id->class-uri       (constantly (str base "Person"))
      #'mbql/database-default-graph    (constantly base)
      #'mbql/database-default-language (constantly "")}
     (fn [] ~@body)))

(deftest compile-base-stage-select-test
  (with-fixture
    (let [{:keys [sparql vars]}
          (compile-stage* {:source-table 100
                           :fields [[:field 1 nil] [:field 2 nil] [:field 3 nil]]})]
      (is (= ["subject" "name" "age"] vars))
      (is (str/includes? sparql "SELECT ?subject ?name ?age"))
      (is (str/includes? sparql (str "?subject a <" base "Person> .")))
      (is (str/includes? sparql (str "OPTIONAL { ?subject <" base "name> ?name . }"))))))

(deftest compile-base-stage-filter-test
  (with-fixture
    (testing "a non-equality filter (e.g. >) stays a bottom FILTER"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 3 nil]]
                             :filter [:> [:field 3 nil] 18]})]
        (is (str/includes? sparql "FILTER (?age > 18)"))
        (is (str/includes? sparql (str "OPTIONAL { ?subject <" base "age> ?age . }")))))
    (testing "a direct equality is pushed into a mandatory anchor triple + BIND (Principle 1)"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 2 nil]]
                             :filter [:= [:field 2 nil] "John"]})]
        (is (str/includes? sparql (str "?subject <" base "name> \"John\" .")))
        (is (str/includes? sparql "BIND(\"John\" AS ?name)"))
        (is (not (str/includes? sparql "FILTER")))
        (is (not (str/includes? sparql (str "OPTIONAL { ?subject <" base "name> ?name . }"))))))))

(deftest compile-base-stage-anchor-test
  (testing "an equality on a direct FK field with a URL value is pushed into a mandatory anchor triple"
    (with-fixture
      (let [iri "https://example.org/countries/AC28-7090"
            {:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 5 nil]]
                             :filter [:= [:field 5 nil] iri]})]
        (is (str/includes? sparql (str "?subject <" base "country> <" iri "> .")))
        (is (str/includes? sparql (str "BIND(<" iri "> AS ?country)")))
        (is (not (str/includes? sparql (str "OPTIONAL { ?subject <" base "country> ?country . }"))))
        (is (not (str/includes? sparql "FILTER")))
        (is (= ["subject" "country"] vars))))))

(deftest compile-base-stage-anchor-and-residual-test
  (testing ":and pushes the anchorable := and keeps the rest as a bottom FILTER"
    (with-fixture
      (let [iri "https://example.org/countries/AC28-7090"
            {:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 3 nil] [:field 5 nil]]
                             :filter [:and
                                      [:= [:field 5 nil] iri]
                                      [:> [:field 3 nil] 18]]})]
        (is (str/includes? sparql (str "?subject <" base "country> <" iri "> .")))
        (is (str/includes? sparql (str "BIND(<" iri "> AS ?country)")))
        (is (str/includes? sparql "FILTER (?age > 18)"))
        (is (not (str/includes? sparql "?country =")))))))

(deftest compile-base-stage-order-limit-test
  (with-fixture
    (let [{:keys [sparql]}
          (compile-stage* {:source-table 100
                           :fields [[:field 1 nil] [:field 2 nil]]
                           :order-by [[:asc [:field 2 nil]]]
                           :limit 10})]
      (is (str/includes? sparql "ORDER BY ASC(?name)"))
      (is (str/includes? sparql "LIMIT 10")))))

(deftest compile-base-stage-aggregation-test
  (with-fixture
    (testing "count with a breakout produces a DISTINCT subject count + GROUP BY"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :aggregation [[:count]]
                             :breakout [[:field 2 nil]]})]
        (is (= ["name" "ag_0"] vars))
        (is (str/includes? sparql "(COUNT(DISTINCT ?subject) AS ?ag_0)"))
        (is (str/includes? sparql "GROUP BY ?name"))))
    (testing "sum aggregates the requested field"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :aggregation [[:sum [:field 3 nil]]]
                             :breakout [[:field 2 nil]]})]
        (is (= ["name" "ag_0"] vars))
        (is (str/includes? sparql "(SUM(?age) AS ?ag_0)"))))))

(deftest compile-base-stage-fk-join-test
  (with-fixture
    (testing "an implicit FK join emits a pair of OPTIONAL triples"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil]
                                      [:field 2 nil]
                                      [:field 10 {:join-alias "Place"}]]
                             :joins [{:alias "Place" :fk-field-id 4}]})]
        (is (= ["subject" "name" "Place__label"] vars))
        (is (str/includes? sparql
                           (str "OPTIONAL { ?subject <" base "birthplace> ?Place_subject . }")))
        (is (str/includes? sparql
                           (str "OPTIONAL { ?Place_subject <" base "label> ?Place__label . }")))))))

(deftest compile-base-stage-implicit-join-projection-test
  (testing "Lib's result-metadata strips :lib/join-alias from implicit-joinable
            columns (only `:fk-field-id` remains). The compiler must still project
            the qualified `?<Alias>__<prop>` var, not the unqualified `?prop` —
            otherwise FK display values come back empty in the UI."
    (let [person-tid 100
          geslacht-tid 200
          fields {1  {:name "subject"      :table-id person-tid}
                  20 {:name "geslacht"     :table-id person-tid}
                  40 {:name "value"       :table-id geslacht-tid}}]
      (with-redefs-fn
        {#'mbql/field-id->metadata        (fn [id] (get fields id))
         #'mbql/table-id->class-uri       (constantly (str base "Person"))
         #'mbql/database-default-graph    (constantly base)
         #'mbql/database-default-language (constantly "")}
        (fn []
          (let [stage {:source-table person-tid
                       :fields       [[:field 1 nil]
                                      [:field 20 nil]
                                      [:field 40 {:join-alias "Geslacht__via__geslacht"}]]
                       :joins        [{:alias        "Geslacht__via__geslacht"
                                       :source-table geslacht-tid
                                       :fk-field-id  20}]}
                ;; Lib's expected-cols for the remap column: `:fk-field-id` only,
                ;; no `:lib/join-alias`.
                expected [{:id 1}
                          {:id 20}
                          {:id 40 :fk-field-id 20}]
                {:keys [sparql vars]} (@#'mbql/compile-base-stage stage expected)]
            (testing "the remap column resolves to the qualified join target var"
              (is (= ["subject" "geslacht" "Geslacht__via__geslacht__value"] vars))
              (is (str/includes? sparql "?Geslacht__via__geslacht__value")))
            (testing "no bogus direct triple is emitted for the joined value fid"
              (is (not (str/includes? sparql
                                      (str "OPTIONAL { ?subject <" base "value> ?value . }")))))))))))

(deftest compile-base-stage-explicit-chained-join-test
  (testing "Two-hop EXPLICIT joins from the notebook editor (Item → Provider → Owner).
            The second join's :condition has :join-alias on BOTH sides, so the
            FK side is identified by 'alias ≠ this join's alias'."
    (let [item-tid     100
          provider-tid 200
          owner-tid    300
          fields {1  {:name "subject" :table-id item-tid}
                  20 {:name "provider" :table-id item-tid}
                  21 {:name "subject" :table-id provider-tid}
                  30 {:name "owner" :table-id provider-tid}
                  31 {:name "subject" :table-id owner-tid}
                  40 {:name "owner_name" :table-id owner-tid}}]
      (with-redefs-fn
        {#'mbql/field-id->metadata        (fn [id] (get fields id))
         #'mbql/table-id->class-uri       (constantly (str base "Item"))
         #'mbql/database-default-graph    (constantly base)
         #'mbql/database-default-language (constantly "")}
        (fn []
          (let [{:keys [sparql]}
                (compile-stage*
                 {:source-table item-tid
                  :aggregation  [[:distinct [:field 1 nil]]]
                  :breakout     [[:field 40 {:join-alias "Owner"}]]
                  :joins        [{:alias        "Provider"
                                  :source-table provider-tid
                                  :condition    [:= [:field 20 nil]
                                                 [:field 21 {:join-alias "Provider"}]]}
                                 {:alias        "Owner"
                                  :source-table owner-tid
                                  :condition    [:= [:field 30 {:join-alias "Provider"}]
                                                 [:field 31 {:join-alias "Owner"}]]}]})]
            (testing "Item → Provider hop"
              (is (str/includes? sparql
                                 (str "OPTIONAL { ?subject <" base "provider> ?Provider_subject . }"))))
            (testing "Provider → Owner hop (the bug fix — explicit-join chained case)"
              (is (str/includes? sparql
                                 (str "OPTIONAL { ?Provider_subject <" base "owner> ?Owner_subject . }"))))
            (testing "leaf property triple"
              (is (str/includes? sparql
                                 (str "OPTIONAL { ?Owner_subject <" base "owner_name> ?Owner__owner_name . }"))))))))))

(deftest compile-base-stage-explicit-chained-join-without-table-id-test
  (testing "Same chained explicit join, but `field-id->metadata` returns NO `:table-id`
            (mirrors a real Metabase metadata-provider result). The FK source alias
            must still be recovered from the condition's `:join-alias` on the FK token,
            not from the metadata's table-id."
    (let [item-tid     100
          provider-tid 200
          owner-tid    300
          ;; No :table-id on any of these — only :name.
          fields {1  {:name "subject"}
                  20 {:name "provider"}
                  21 {:name "subject"}
                  30 {:name "owner"}
                  31 {:name "subject"}
                  40 {:name "owner_name"}}]
      (with-redefs-fn
        {#'mbql/field-id->metadata        (fn [id] (get fields id))
         #'mbql/table-id->class-uri       (constantly (str base "Item"))
         #'mbql/database-default-graph    (constantly base)
         #'mbql/database-default-language (constantly "")}
        (fn []
          (let [{:keys [sparql]}
                (compile-stage*
                 {:source-table item-tid
                  :aggregation  [[:distinct [:field 1 nil]]]
                  :breakout     [[:field 40 {:join-alias "Owner"}]]
                  :joins        [{:alias        "Provider"
                                  :source-table provider-tid
                                  :condition    [:= [:field 20 nil]
                                                 [:field 21 {:join-alias "Provider"}]]}
                                 {:alias        "Owner"
                                  :source-table owner-tid
                                  :condition    [:= [:field 30 {:join-alias "Provider"}]
                                                 [:field 31 {:join-alias "Owner"}]]}]})]
            (is (str/includes? sparql
                               (str "OPTIONAL { ?Provider_subject <" base "owner> ?Owner_subject . }"))
                "Owner FK triple must be anchored on ?Provider_subject even without :table-id metadata")))))))

(deftest compile-base-stage-chained-fk-join-test
  (testing "a 2-hop implicit join chain (Item → Provider → Owner) anchors the second
            join's FK triple to the first join's intermediate var, not ?subject"
    (let [fields {1  {:name "subject"}
                  20 {:name "provider"   :table-id 100}
                  30 {:name "owner"      :table-id 200}
                  40 {:name "owner_name" :table-id 300}}]
      (with-redefs-fn
        {#'mbql/field-id->metadata        (fn [id] (get fields id))
         #'mbql/table-id->class-uri       (constantly (str base "Item"))
         #'mbql/database-default-graph    (constantly base)
         #'mbql/database-default-language (constantly "")}
        (fn []
          (let [{:keys [sparql]}
                (compile-stage*
                 {:source-table 100
                  :aggregation  [[:count]]
                  :breakout     [[:field 40 {:join-alias "Owner"}]]
                  :joins        [{:alias "Provider" :source-table 200 :fk-field-id 20}
                                 {:alias "Owner"    :source-table 300 :fk-field-id 30}]})]
            (testing "Item → Provider hop is anchored on ?subject"
              (is (str/includes? sparql
                                 (str "OPTIONAL { ?subject <" base "provider> ?Provider_subject . }"))))
            (testing "Provider → Owner hop is anchored on ?Provider_subject (the bug fix)"
              (is (str/includes? sparql
                                 (str "OPTIONAL { ?Provider_subject <" base "owner> ?Owner_subject . }"))))
            (testing "leaf property triple anchors on ?Owner_subject"
              (is (str/includes? sparql
                                 (str "OPTIONAL { ?Owner_subject <" base "owner_name> ?Owner__owner_name . }"))))))))))

(deftest compile-derived-stage-aggregation-test
  (with-fixture
    (testing "an aggregation layered on a saved card compiles to a single column"
      ;; This is the regression case: a 'Minimum of Count' on a count-by-breakout card.
      (let [card {:source-table 100 :aggregation [[:count]] :breakout [[:field 2 nil]]}
            {:keys [sparql vars]}
            (compile-stage* {:source-query card
                             :aggregation [[:min [:field "ag_0" nil]]]})]
        (is (= ["ag_0"] vars))
        (is (str/includes? sparql "(MIN(?ag_0) AS ?ag_0)"))
        (testing "the outer stage adds no GROUP BY (only the inner card's remains)"
          (is (= 1 (count (re-seq #"GROUP BY" sparql)))))))))

(deftest compile-derived-stage-passthrough-test
  (with-fixture
    (testing "with no outer clauses the inner card columns pass straight through"
      (let [card {:source-table 100 :aggregation [[:count]] :breakout [[:field 2 nil]]}
            {:keys [vars]} (compile-stage* {:source-query card})]
        (is (= ["name" "ag_0"] vars))))))

(deftest compile-derived-stage-outer-filter-test
  (with-fixture
    (testing "an outer filter on a saved card is applied around the sub-SELECT"
      (let [card {:source-table 100 :aggregation [[:count]] :breakout [[:field 2 nil]]}
            {:keys [sparql vars]}
            (compile-stage* {:source-query card
                             :filter [:= [:field "name" nil] "John"]})]
        (is (= ["name" "ag_0"] vars))
        (is (str/includes? sparql "FILTER (?name = \"John\")"))))))

(deftest compile-derived-stage-filter-on-aggregation-test
  (with-fixture
    (testing "drilling on an aggregation value (filter references it by Lib's name) is applied"
      (let [card {:source-table 100 :aggregation [[:count]] :breakout [[:field 2 nil]]}
            {:keys [sparql vars]}
            (compile-stage* {:source-query card
                             :filter [:< [:field "count" nil] 12]})]
        (is (= ["name" "ag_0"] vars))
        (is (str/includes? sparql "FILTER (?ag_0 < 12)")
            "the `count` column reference resolves to the ?ag_0 SPARQL variable")))
    (testing "a named aggregation resolves by its :aggregation-options name"
      (let [card {:source-table 100
                  :aggregation [[:aggregation-options [:sum [:field 3 nil]] {:name "total"}]]
                  :breakout    [[:field 2 nil]]}
            {:keys [sparql]}
            (compile-stage* {:source-query card
                             :filter [:>= [:field "total" nil] 100]})]
        (is (str/includes? sparql "FILTER (?ag_0 >= 100)"))))
    (testing "duplicate aggregation names are disambiguated count, count_2, …"
      (let [f @#'mbql/aggregation-name->var]
        (is (= {"count" "ag_0" "count_2" "ag_1"}
               (f [[:count] [:distinct [:field 3 nil]]])))))))

(deftest compile-derived-stage-filter-on-joined-breakout-test
  (with-fixture
    (testing "an outer filter on a joined breakout column resolves via Lib's expected-cols name"
      (let [card {:source-table 100
                  :aggregation [[:count]]
                  :breakout    [[:field 10 {:join-alias "Place"}]]
                  :joins       [{:alias "Place" :fk-field-id 4}]}
            ;; Lib names the joined breakout column "Birthplace" — different from the
            ;; driver's invented SPARQL var "Place__label".
            expected [{:name "Birthplace"} {:name "count"}]
            {:keys [sparql vars]}
            (@#'mbql/compile-derived-stage
             {:source-query card
              :filter [:= [:field "Birthplace" nil] "Leuven"]}
             expected)]
        (is (= ["Place__label" "ag_0"] vars))
        (is (str/includes? sparql "FILTER (?Place__label = \"Leuven\")")
            "the Lib column name resolves to the joined SPARQL variable")))
    (testing "the same name-aliasing applies to order-by on a joined breakout column"
      (let [card {:source-table 100
                  :aggregation [[:count]]
                  :breakout    [[:field 10 {:join-alias "Place"}]]
                  :joins       [{:alias "Place" :fk-field-id 4}]}
            expected [{:name "Birthplace"} {:name "count"}]
            {:keys [sparql]}
            (@#'mbql/compile-derived-stage
             {:source-query card
              :order-by [[:asc [:field "Birthplace" nil]]]}
             expected)]
        (is (str/includes? sparql "ORDER BY ASC(?Place__label)"))))))

(deftest compile-derived-stage-outer-count-test
  (with-fixture
    (testing "an arg-less count on a derived stage uses COUNT(*) (no ?subject available)"
      (let [card {:source-table 100 :aggregation [[:count]] :breakout [[:field 2 nil]]}
            {:keys [sparql vars]}
            (compile-stage* {:source-query card
                             :aggregation [[:count]]
                             :breakout [[:field "name" nil]]})]
        (is (= ["name" "ag_0"] vars))
        (is (str/includes? sparql "(COUNT(*) AS ?ag_0)"))
        (is (str/includes? sparql "GROUP BY ?name"))))))

;; ---------------------------------------------------------------------------
;; Lib-driven projection (the column-count-mismatch fix)
;; ---------------------------------------------------------------------------

(deftest reconcile-base-projection-test
  (with-fixture
    (let [f   @#'mbql/reconcile-base-projection
          ctx {:field-id->var           {2 "name" 3 "age"}
               :pair->target-var        {[10 "Place"] "Place__label"}
               :alias->intermediate-var {"Place" "Place_subject"}
               :default-graph           base}]
      (testing "columns the compiler already projects reuse their variable"
        (is (= {:vars ["subject" "name" "Place__label"] :triples []}
               (f [{:id 1} {:id 2} {:id 10 :lib/join-alias "Place"}] ctx))))
      (testing "a joined column the compiler missed is synthesized off the intermediate var"
        (is (= {:vars    ["subject" "Place__name"]
                :triples [(str "  OPTIONAL { ?Place_subject <" base "name> ?Place__name . }")]}
               (f [{:id 1} {:id 2 :lib/join-alias "Place"}] ctx))))
      (testing "an unresolvable column still gets a (placeholder) variable"
        (is (= {:vars ["undefined_1"] :triples []}
               (f [{:lib/join-alias "Nope"}] ctx)))))))

(deftest compile-base-stage-lib-driven-projection-test
  (with-fixture
    (testing "the SELECT is reconciled against Lib's expected columns"
      (let [stage    {:source-table 100
                      :fields [[:field 1 nil] [:field 2 nil]
                               [:field 10 {:join-alias "Place"}]]
                      :joins  [{:alias "Place" :fk-field-id 4}]}
            ;; Lib expects an extra joined column (id 3) the :fields list omits —
            ;; the FK-remap-on-a-remap case behind the recurring column mismatch.
            expected [{:id 1} {:id 2}
                      {:id 10 :lib/join-alias "Place"}
                      {:id 3  :lib/join-alias "Place"}]
            {:keys [sparql vars]} (@#'mbql/compile-base-stage stage expected)]
        (is (= 4 (count vars)) "one SELECT variable per Lib expected column")
        (is (= ["subject" "name" "Place__label" "Place__age"] vars))
        (is (str/includes? sparql "SELECT ?subject ?name ?Place__label ?Place__age"))
        (testing "the missing column is synthesized off the join's intermediate var"
          (is (str/includes?
               sparql
               (str "OPTIONAL { ?Place_subject <" base "age> ?Place__age . }"))))))))

(deftest compile-base-stage-lib-driven-order-test
  (with-fixture
    (testing "expected-cols drives column order, independent of :fields order"
      (let [stage {:source-table 100
                   :fields [[:field 1 nil] [:field 2 nil] [:field 3 nil]]}
            {:keys [vars]} (@#'mbql/compile-base-stage stage [{:id 1} {:id 3} {:id 2}])]
        (is (= ["subject" "age" "name"] vars))))))

(deftest compile-derived-stage-lib-driven-projection-test
  (with-fixture
    (testing "expected-cols drives the derived-stage SELECT and preserves the column count"
      (let [card {:source-table 100 :aggregation [[:count]] :breakout [[:field 2 nil]]}
            ;; the inner card projects [name ag_0]; Lib expects a third column
            {:keys [vars]} (@#'mbql/compile-derived-stage
                            {:source-query card}
                            [{:id 2} {:id 3} {:id 99 :lib/join-alias "Missing"}])]
        (is (= 3 (count vars)))
        (is (= ["name" "ag_0"] (take 2 vars)))
        (is (str/starts-with? (last vars) "undefined_"))))))

(deftest compile-base-stage-lang-filter-test
  (testing "rdf:langString columns get a LANG filter when a default language is set"
    (with-redefs-fn
      {#'mbql/field-id->metadata        (fn [id] (get {1 {:name "subject"}
                                                       2 {:name "name" :database-type "langString"}}
                                                      id))
       #'mbql/table-id->class-uri       (constantly (str base "Person"))
       #'mbql/database-default-graph    (constantly base)
       #'mbql/database-default-language (constantly "nl")}
      (fn []
        (let [{:keys [sparql]}
              (compile-stage* {:source-table 100
                               :fields [[:field 1 nil] [:field 2 nil]]})]
          (is (str/includes?
               sparql
               "FILTER(!BOUND(?name) || LANG(?name) = \"nl\" || LANG(?name) = \"\")")))))))

;; ---------------------------------------------------------------------------
;; Geometry / WKT filtering
;; ---------------------------------------------------------------------------

(deftest geometry-equality-uses-str-test
  (with-fixture
    (testing "= on a geometry field compares lexical form via STR() (typed literal never = plain string)"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 11 nil]]
                             :filter [:= [:field 11 nil] "POINT(4.70 50.88)"]})]
        (is (str/includes? sparql "FILTER (STR(?location) = \"POINT(4.70 50.88)\")"))
        (is (not (str/includes? sparql "(?location = ")))))
    (testing "!= on a geometry field also uses STR()"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 11 nil]]
                             :filter [:!= [:field 11 nil] "POINT(4.70 50.88)"]})]
        (is (str/includes? sparql "FILTER (STR(?location) != \"POINT(4.70 50.88)\")"))))
    (testing "a geometry equality is NOT pushed into a mandatory BGP anchor triple"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 11 nil]]
                             :filter [:= [:field 11 nil] "POINT(4.70 50.88)"]})]
        (is (not (str/includes? sparql "?subject <https://example.org/location> \"POINT")))))))

;; ---------------------------------------------------------------------------
;; Custom expressions (custom columns) → SPARQL
;; ---------------------------------------------------------------------------

(deftest compile-expression-test
  (let [resolve-token (fn [tok]
                        (if (= :expression (first tok))
                          (@#'mbql/sanitize-var-name (second tok))
                          (get {1 "a" 2 "b"} (second tok) (str (second tok)))))
        f (fn [clause] (@#'mbql/compile-expression clause resolve-token))]
    (testing "arithmetic"
      (is (= "(?a + 1)" (f [:+ [:field 1 nil] 1])))
      (is (= "(?a - ?b)" (f [:- [:field 1 nil] [:field 2 nil]])))
      (is (= "(?a * 2)" (f [:* [:field 1 nil] 2]))))
    (testing "string functions coerce args with STR()"
      (is (= "LCASE(STR(?a))" (f [:lower [:field 1 nil]])))
      (is (= "STRLEN(STR(?a))" (f [:length [:field 1 nil]])))
      (is (= "CONCAT(STR(?a), STR(?b))" (f [:concat [:field 1 nil] [:field 2 nil]]))))
    (testing "trim compiles to a REPLACE"
      (is (= "REPLACE(STR(?a), \"^\\\\s+|\\\\s+$\", \"\")" (f [:trim [:field 1 nil]]))))
    (testing "regexextract compiles to a first-match REPLACE"
      (is (= "REPLACE(STR(?a), \"^.*?([-0-9.]+).*$\", \"$1\")"
             (f [:regex-match-first [:field 1 nil] "[-0-9.]+"]))))
    (testing "substring is 1-based SUBSTR"
      (is (= "SUBSTR(STR(?a), 2, 3)" (f [:substring [:field 1 nil] 2 3])))
      (is (= "SUBSTR(STR(?a), 2)" (f [:substring [:field 1 nil] 2]))))
    (testing "casts use the full xsd IRI constructor"
      (is (= "<http://www.w3.org/2001/XMLSchema#double>(?a)" (f [:float [:field 1 nil]])))
      (is (= "<http://www.w3.org/2001/XMLSchema#integer>(?a)" (f [:integer [:field 1 nil]]))))
    (testing "coalesce / case"
      (is (= "COALESCE(?a, \"x\")" (f [:coalesce [:field 1 nil] "x"])))
      (is (= "IF((?a > 5), \"big\", \"small\")"
             (f [:case [[[:> [:field 1 nil] 5] "big"]] {:default "small"}]))))
    (testing "an unsupported function throws a clear error"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unsupported expression function"
                            (f [:totally-bogus [:field 1 nil]]))))))

(deftest compile-base-stage-expression-test
  (with-fixture
    (testing "a custom column emits a BIND and is projected"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 2 nil] [:expression "upper_name"]]
                             :expressions {"upper_name" [:upper [:field 2 nil]]}})]
        (is (some #{"upper_name"} vars))
        (is (str/includes? sparql "BIND(UCASE(STR(?name)) AS ?upper_name)"))
        (is (str/includes? sparql "SELECT ?subject ?name ?upper_name"))))
    (testing "a field referenced only inside an expression still gets its triple"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:expression "len"]]
                             :expressions {"len" [:length [:field 3 nil]]}})]
        (is (str/includes? sparql "OPTIONAL { ?subject <https://example.org/age> ?age . }"))
        (is (str/includes? sparql "BIND(STRLEN(STR(?age)) AS ?len)"))))
    (testing "filter and order-by can reference an expression"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:expression "len"]]
                             :expressions {"len" [:length [:field 2 nil]]}
                             :filter [:> [:expression "len"] 3]
                             :order-by [[:desc [:expression "len"]]]})]
        (is (str/includes? sparql "FILTER (?len > 3)"))
        (is (str/includes? sparql "ORDER BY DESC(?len)"))))))

(deftest compile-base-stage-lonlat-recipe-test
  (with-fixture
    (testing "the documented lon/lat recipe over a geometry column compiles end-to-end"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:expression "lon"] [:expression "lat"]]
                             :expressions {"lon" [:float [:regex-match-first [:field 11 nil] "[-0-9.]+"]]
                                           "lat" [:float [:trim [:regex-match-first [:field 11 nil] " [-0-9.]+"]]]}})]
        (is (= #{"lon" "lat"} (set (filter #{"lon" "lat"} vars))))
        (is (str/includes? sparql "OPTIONAL { ?subject <https://example.org/location> ?location . }"))
        (is (str/includes?
             sparql
             "BIND(<http://www.w3.org/2001/XMLSchema#double>(REPLACE(STR(?location), \"^.*?([-0-9.]+).*$\", \"$1\")) AS ?lon)"))
        (testing "lat = float(trim(regexextract(...))) wraps the inner match in a trim REPLACE"
          (is (str/includes? sparql "\"^\\\\s+|\\\\s+$\", \"\")"))
          (is (str/includes? sparql "^.*?( [-0-9.]+).*$")))))))

;; ---------------------------------------------------------------------------
;; Synthesized geometry coordinate columns (auto lon/lat, BOX corners)
;; ---------------------------------------------------------------------------

(deftest geo-coord-marker-test
  (with-fixture
    (let [marker @#'mbql/geo-coord-marker
          geo?   @#'mbql/geo-coord-field?]
      (is (= {:axis "point-lon" :source "location"} (marker 12)))
      (is (= {:axis "box-min-lon" :source "bbox"} (marker 14)))
      (is (geo? 12))
      (is (not (geo? 11)))   ;; raw geometry column is not itself a coordinate
      (is (not (geo? 2))))))

(deftest geo-coord-point-compile-test
  (with-fixture
    (testing "selecting synthesized lon/lat emits ONE source triple + two extraction BINDs"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 12 nil] [:field 13 nil]]})]
        (is (= ["subject" "location_lon" "location_lat"] vars))
        (is (str/includes? sparql "OPTIONAL { ?subject <https://example.org/location> ?location . }"))
        (is (str/includes?
             sparql
             "BIND(<http://www.w3.org/2001/XMLSchema#double>(REPLACE(STR(?location), "))
        (is (str/includes? sparql "POINT"))
        (is (str/includes? sparql "\"$1\")) AS ?location_lon)"))
        (is (str/includes? sparql "\"$2\")) AS ?location_lat)"))
        (testing "no bogus property triple for the coordinate columns themselves"
          (is (not (str/includes? sparql "location_lon>")))
          (is (not (str/includes? sparql "location_lat>"))))))
    (testing "the source geometry triple is shared (emitted once) for lon+lat"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 12 nil] [:field 13 nil]]})]
        (is (= 1 (count (re-seq #"<https://example\.org/location> \?location " sparql))))))))

(deftest geo-coord-box-compile-test
  (with-fixture
    (testing "BOX corners extract via a 4-group REPLACE off the bbox source"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 14 nil] [:field 15 nil]]})]
        (is (= ["subject" "bbox_min_lon" "bbox_max_lat"] vars))
        (is (str/includes? sparql "<https://example.org/bbox> ?bbox"))
        (is (str/includes? sparql "BOX"))
        (is (str/includes? sparql "\"$1\")) AS ?bbox_min_lon)"))
        (is (str/includes? sparql "\"$4\")) AS ?bbox_max_lat)"))))))

(deftest geo-coord-joined-compile-test
  (with-fixture
    (testing "a synthesized coordinate reached via an implicit join binds off the join var"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 12 {:join-alias "Place"}]]
                             :joins  [{:alias "Place" :fk-field-id 4}]})]
        ;; FK + joined source geometry triple, then the BIND off the joined source var
        (is (str/includes? sparql "OPTIONAL { ?subject <https://example.org/birthplace> ?Place_subject . }"))
        (is (str/includes? sparql "OPTIONAL { ?Place_subject <https://example.org/location> ?Place__location . }"))
        (is (str/includes? sparql "REPLACE(STR(?Place__location), "))
        (is (str/includes? sparql "AS ?Place__location_lon)"))))))

(deftest geo-coord-filter-test
  (with-fixture
    (testing "a synthesized coordinate can be filtered numerically (and its BIND is emitted)"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 12 nil]]
                             :filter [:< [:field 12 nil] 5]})]
        (is (str/includes? sparql "AS ?location_lon)"))
        (is (str/includes? sparql "FILTER (?location_lon < 5)"))))))

(deftest inside-and-between-filter-test
  (with-fixture
    (testing ":between compiles to a numeric range"
      (let [f #(@#'mbql/compile-filter-expr % {"age" "age"} {})]
        (is (= "(?age >= 18 && ?age <= 65)"
               (f [:between [:field "age" nil] 18 65])))
        (testing "a [:value ...] wrapped bound is unwrapped"
          (is (= "(?age >= 18 && ?age <= 65)"
                 (f [:between [:field "age" nil] [:value 18 {}] [:value 65 {}]]))))))
    (testing ":inside compiles to a bounding box over lat/lon (N/W/S/E order)"
      (let [f #(@#'mbql/compile-filter-expr % {"lat" "lat" "lon" "lon"} {})]
        (is (= "((?lat <= 51.0) && (?lat >= 50.0) && (?lon >= 4.0) && (?lon <= 5.0))"
               (f [:inside [:field "lat" nil] [:field "lon" nil] 51.0 4.0 50.0 5.0])))))
    (testing ":inside on synthesized coordinate columns emits their BINDs + the box FILTER"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 12 nil] [:field 13 nil]]
                             :filter [:inside [:field 13 nil] [:field 12 nil] 51.0 4.0 50.0 5.0]})]
        (is (str/includes? sparql "OPTIONAL { ?subject <https://example.org/location> ?location . }"))
        (is (str/includes? sparql "AS ?location_lon)"))
        (is (str/includes? sparql "AS ?location_lat)"))
        (is (str/includes?
             sparql
             "FILTER ((?location_lat <= 51.0) && (?location_lat >= 50.0) && (?location_lon >= 4.0) && (?location_lon <= 5.0))"))))))

;; ---------------------------------------------------------------------------
;; Binning (grid / heat maps, numeric bins)
;; ---------------------------------------------------------------------------

(deftest bin-expr-test
  (let [f @#'mbql/bin-expr]
    (testing "anchored at zero drops the min offset"
      (is (= "(FLOOR(?lon / 0.1) * 0.1)" (f "lon" 0.1 0))))
    (testing "non-zero min uses the full floor formula"
      (is (= "((FLOOR((?age - 10) / 10) * 10) + 10)" (f "age" 10 10))))))

(deftest binned-breakout-compile-test
  (with-fixture
    (testing "a binned numeric breakout buckets the column and groups by the bucket var"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :breakout    [[:field 3 {:binning {:strategy :bin-width :bin-width 10 :min-value 0}}]]
                             :aggregation [[:count]]})]
        (is (= ["age_binned" "ag_0"] vars))
        (is (str/includes? sparql "OPTIONAL { ?subject <https://example.org/age> ?age . }"))
        (is (str/includes? sparql "BIND((FLOOR(?age / 10) * 10) AS ?age_binned)"))
        (is (str/includes? sparql "GROUP BY ?age_binned"))
        (is (str/includes? sparql "(COUNT(DISTINCT ?subject) AS ?ag_0)"))))))

(deftest grid-map-binned-coordinates-test
  (with-fixture
    (testing "grid map: binned synthesized lon/lat + count -> coordinate BINDs, bin BINDs, grouped"
      (let [bin {:strategy :bin-width :bin-width 0.1 :min-value 0}
            {:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :breakout    [[:field 12 {:binning bin}] [:field 13 {:binning bin}]]
                             :aggregation [[:count]]})]
        (is (= ["location_lon_binned" "location_lat_binned" "ag_0"] vars))
        ;; one shared source geometry triple
        (is (str/includes? sparql "OPTIONAL { ?subject <https://example.org/location> ?location . }"))
        ;; coordinate extraction BINDs feed the bin BINDs
        (is (str/includes? sparql "AS ?location_lon)"))
        (is (str/includes? sparql "AS ?location_lat)"))
        (is (str/includes? sparql "BIND((FLOOR(?location_lon / 0.1) * 0.1) AS ?location_lon_binned)"))
        (is (str/includes? sparql "BIND((FLOOR(?location_lat / 0.1) * 0.1) AS ?location_lat_binned)"))
        (is (str/includes? sparql "GROUP BY ?location_lon_binned ?location_lat_binned"))))))
