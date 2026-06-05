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
    (is (= "naam" (f "naam")))
    (is (= "geboorte_plaats" (f "geboorte-plaats")))
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
    (is (= "naam" (id [:field "naam" nil])))
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
  (let [f #(@#'mbql/compile-filter-expr % {"naam" "naam" "leeftijd" "leeftijd"} {})]
    (is (= "(?naam = \"Jan\")"        (f [:= [:field "naam" nil] "Jan"])))
    (testing "a wrapped [:value ...] rhs is unwrapped"
      (is (= "(?naam = \"Jan\")"      (f [:= [:field "naam" nil] [:value "Jan" {}]]))))
    (is (= "(!BOUND(?naam))"          (f [:= [:field "naam" nil] nil])))
    (is (= "(BOUND(?naam))"           (f [:!= [:field "naam" nil] nil])))
    (is (= "(?leeftijd > 18)"         (f [:> [:field "leeftijd" nil] 18])))
    (is (= "(?naam != \"Jan\")"       (f [:!= [:field "naam" nil] "Jan"])))
    (testing "case-insensitive contains"
      (is (= "(CONTAINS(LCASE(STR(?naam)), LCASE(\"an\")))"
             (f [:contains [:field "naam" nil] "an" {:case-sensitive false}]))))
    (testing "boolean combinators"
      (is (= "((?naam = \"Jan\") && (?leeftijd > 18))"
             (f [:and [:= [:field "naam" nil] "Jan"] [:> [:field "leeftijd" nil] 18]])))
      (is (= "((?naam = \"Jan\") || (?naam = \"Piet\"))"
             (f [:or [:= [:field "naam" nil] "Jan"] [:= [:field "naam" nil] "Piet"]]))))
    (testing "IRI-valued fields render a URL value as <IRI>, not a string literal"
      (with-redefs [mbql/field-id->metadata {5 {:name "archiefcode" :semantic-type :type/FK}
                                             6 {:name "homepage" :semantic-type :type/URL}
                                             2 {:name "naam" :database-type "string"}}]
        (let [g #(@#'mbql/compile-filter-expr % {5 "archiefcode" 6 "homepage" 2 "naam"} {})
              iri "https://odis.q.libis.be/archiefcodes/AC28-7090"]
          (is (= (str "(?archiefcode = <" iri ">)")
                 (g [:= [:field 5 nil] iri]))
              "FK field + URL value → IRI term")
          (is (= (str "(?homepage != <" iri ">)")
                 (g [:!= [:field 6 nil] iri]))
              "URL field + URL value → IRI term")
          (is (= "(?archiefcode = \"AC-123\")"
                 (g [:= [:field 5 nil] "AC-123"]))
              "FK field + non-URL value stays a string literal")
          (is (= (str "(?naam = \"" iri "\")")
                 (g [:= [:field 2 nil] iri]))
              "plain string field + URL-shaped value stays a string literal"))))))

(deftest order-by-test
  (let [ob     #(@#'mbql/compile-order-by % {"naam" "naam" "leeftijd" "leeftijd"} {})
        agg-ob #(@#'mbql/compile-agg-order-by % {"naam" "naam"} {})]
    (is (= "ORDER BY ASC(?naam)" (ob [[:asc [:field "naam" nil]]])))
    (is (= "ORDER BY DESC(?naam) ASC(?leeftijd)"
           (ob [[:desc [:field "naam" nil]] [:asc [:field "leeftijd" nil]]])))
    (is (nil? (ob [])))
    (testing "aggregation order-by can reference an aggregation by index"
      (is (= "ORDER BY DESC(?ag_0)" (agg-ob [[:desc [:aggregation 0]]])))
      (is (= "ORDER BY ASC(?naam)"  (agg-ob [[:asc [:field "naam" nil]]]))))))

(deftest var-for-token-test
  (let [f @#'mbql/var-for-token]
    (is (= "naam" (f [:field "naam" nil] {"naam" "naam"} {})))
    (testing "a join-alias token resolves through pair->target-var"
      (is (= "jvar" (f [:field "x" {:join-alias "J"}] {} {["x" "J"] "jvar"}))))))

(deftest inner-var-for-ref-test
  (let [f @#'mbql/inner-var-for-ref]
    (testing "a source-query column (string id) resolves to its sanitized name"
      (is (= "ag_0" (f [:field "ag_0" nil])))
      (is (= "geboorte_plaats" (f [:field "geboorte-plaats" nil]))))))

;; ---------------------------------------------------------------------------
;; Stage compilation (metadata accessors stubbed)
;; ---------------------------------------------------------------------------

(def ^:private base "https://odis.q.libis.be/")

(def ^:private fixture-fields
  {1  {:name "subject"}
   2  {:name "naam" :database-type "string"}
   3  {:name "leeftijd" :database-type "string"}
   4  {:name "geboorteplaats" :database-type "string"}
   5  {:name "archiefcode" :database-type "string"
       :semantic-type :type/FK :fk-target-class (str base "Archiefcode")}
   6  {:name "homepage" :database-type "string" :base-type :type/URL :semantic-type :type/URL}
   10 {:name "label" :database-type "string"}
   11 {:name "lokatie" :database-type "geometry"}
   12 {:name "lokatie_lon" :database-type "geo-coord:point-lon:lokatie"
       :base-type :type/Float :semantic-type :type/Longitude}
   13 {:name "lokatie_lat" :database-type "geo-coord:point-lat:lokatie"
       :base-type :type/Float :semantic-type :type/Latitude}
   14 {:name "begrenzingsvak_min_lon" :database-type "geo-coord:box-min-lon:begrenzingsvak"
       :base-type :type/Float :semantic-type :type/Coordinate}
   15 {:name "begrenzingsvak_max_lat" :database-type "geo-coord:box-max-lat:begrenzingsvak"
       :base-type :type/Float :semantic-type :type/Coordinate}})

(defn- compile-stage* [stage]
  (@#'mbql/compile-stage stage))

(defmacro ^:private with-fixture
  "Run `body` with the four metadata accessors stubbed for the ODIS fixture."
  [& body]
  `(with-redefs-fn
     {#'mbql/field-id->metadata        (fn [id#] (get fixture-fields id#))
      #'mbql/table-id->class-uri       (constantly (str base "Persoon"))
      #'mbql/database-default-graph    (constantly base)
      #'mbql/database-default-language (constantly "")}
     (fn [] ~@body)))

(deftest compile-base-stage-select-test
  (with-fixture
    (let [{:keys [sparql vars]}
          (compile-stage* {:source-table 100
                           :fields [[:field 1 nil] [:field 2 nil] [:field 3 nil]]})]
      (is (= ["subject" "naam" "leeftijd"] vars))
      (is (str/includes? sparql "SELECT ?subject ?naam ?leeftijd"))
      (is (str/includes? sparql (str "?subject a <" base "Persoon> .")))
      (is (str/includes? sparql (str "OPTIONAL { ?subject <" base "naam> ?naam . }"))))))

(deftest compile-base-stage-filter-test
  (with-fixture
    (testing "a non-equality filter (e.g. >) stays a bottom FILTER"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 3 nil]]
                             :filter [:> [:field 3 nil] 18]})]
        (is (str/includes? sparql "FILTER (?leeftijd > 18)"))
        (is (str/includes? sparql (str "OPTIONAL { ?subject <" base "leeftijd> ?leeftijd . }")))))
    (testing "a direct equality is pushed into a mandatory anchor triple + BIND (Principle 1)"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 2 nil]]
                             :filter [:= [:field 2 nil] "Jan"]})]
        (is (str/includes? sparql (str "?subject <" base "naam> \"Jan\" .")))
        (is (str/includes? sparql "BIND(\"Jan\" AS ?naam)"))
        (is (not (str/includes? sparql "FILTER")))
        (is (not (str/includes? sparql (str "OPTIONAL { ?subject <" base "naam> ?naam . }"))))))))

(deftest compile-base-stage-anchor-test
  (testing "an equality on a direct FK field with a URL value is pushed into a mandatory anchor triple"
    (with-fixture
      (let [iri "https://odis.q.libis.be/archiefcodes/AC28-7090"
            {:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 5 nil]]
                             :filter [:= [:field 5 nil] iri]})]
        (is (str/includes? sparql (str "?subject <" base "archiefcode> <" iri "> .")))
        (is (str/includes? sparql (str "BIND(<" iri "> AS ?archiefcode)")))
        (is (not (str/includes? sparql (str "OPTIONAL { ?subject <" base "archiefcode> ?archiefcode . }"))))
        (is (not (str/includes? sparql "FILTER")))
        (is (= ["subject" "archiefcode"] vars))))))

(deftest compile-base-stage-anchor-and-residual-test
  (testing ":and pushes the anchorable := and keeps the rest as a bottom FILTER"
    (with-fixture
      (let [iri "https://odis.q.libis.be/archiefcodes/AC28-7090"
            {:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 3 nil] [:field 5 nil]]
                             :filter [:and
                                      [:= [:field 5 nil] iri]
                                      [:> [:field 3 nil] 18]]})]
        (is (str/includes? sparql (str "?subject <" base "archiefcode> <" iri "> .")))
        (is (str/includes? sparql (str "BIND(<" iri "> AS ?archiefcode)")))
        (is (str/includes? sparql "FILTER (?leeftijd > 18)"))
        (is (not (str/includes? sparql "?archiefcode =")))))))

(deftest compile-base-stage-order-limit-test
  (with-fixture
    (let [{:keys [sparql]}
          (compile-stage* {:source-table 100
                           :fields [[:field 1 nil] [:field 2 nil]]
                           :order-by [[:asc [:field 2 nil]]]
                           :limit 10})]
      (is (str/includes? sparql "ORDER BY ASC(?naam)"))
      (is (str/includes? sparql "LIMIT 10")))))

(deftest compile-base-stage-aggregation-test
  (with-fixture
    (testing "count with a breakout produces a DISTINCT subject count + GROUP BY"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :aggregation [[:count]]
                             :breakout [[:field 2 nil]]})]
        (is (= ["naam" "ag_0"] vars))
        (is (str/includes? sparql "(COUNT(DISTINCT ?subject) AS ?ag_0)"))
        (is (str/includes? sparql "GROUP BY ?naam"))))
    (testing "sum aggregates the requested field"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :aggregation [[:sum [:field 3 nil]]]
                             :breakout [[:field 2 nil]]})]
        (is (= ["naam" "ag_0"] vars))
        (is (str/includes? sparql "(SUM(?leeftijd) AS ?ag_0)"))))))

(deftest compile-base-stage-fk-join-test
  (with-fixture
    (testing "an implicit FK join emits a pair of OPTIONAL triples"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil]
                                      [:field 2 nil]
                                      [:field 10 {:join-alias "Plaats"}]]
                             :joins [{:alias "Plaats" :fk-field-id 4}]})]
        (is (= ["subject" "naam" "Plaats__label"] vars))
        (is (str/includes? sparql
                           (str "OPTIONAL { ?subject <" base "geboorteplaats> ?Plaats_subject . }")))
        (is (str/includes? sparql
                           (str "OPTIONAL { ?Plaats_subject <" base "label> ?Plaats__label . }")))))))

(deftest compile-base-stage-implicit-join-projection-test
  (testing "Lib's result-metadata strips :lib/join-alias from implicit-joinable
            columns (only `:fk-field-id` remains). The compiler must still project
            the qualified `?<Alias>__<prop>` var, not the unqualified `?prop` —
            otherwise FK display values come back empty in the UI."
    (let [persoon-tid 100
          geslacht-tid 200
          fields {1  {:name "subject"      :table-id persoon-tid}
                  20 {:name "geslacht"     :table-id persoon-tid}
                  40 {:name "waarde"       :table-id geslacht-tid}}]
      (with-redefs-fn
        {#'mbql/field-id->metadata        (fn [id] (get fields id))
         #'mbql/table-id->class-uri       (constantly (str base "Persoon"))
         #'mbql/database-default-graph    (constantly base)
         #'mbql/database-default-language (constantly "")}
        (fn []
          (let [stage {:source-table persoon-tid
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
              (is (= ["subject" "geslacht" "Geslacht__via__geslacht__waarde"] vars))
              (is (str/includes? sparql "?Geslacht__via__geslacht__waarde")))
            (testing "no bogus direct triple is emitted for the joined waarde fid"
              (is (not (str/includes? sparql
                                      (str "OPTIONAL { ?subject <" base "waarde> ?waarde . }")))))))))))

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
        (is (= ["naam" "ag_0"] vars))))))

(deftest compile-derived-stage-outer-filter-test
  (with-fixture
    (testing "an outer filter on a saved card is applied around the sub-SELECT"
      (let [card {:source-table 100 :aggregation [[:count]] :breakout [[:field 2 nil]]}
            {:keys [sparql vars]}
            (compile-stage* {:source-query card
                             :filter [:= [:field "naam" nil] "Jan"]})]
        (is (= ["naam" "ag_0"] vars))
        (is (str/includes? sparql "FILTER (?naam = \"Jan\")"))))))

(deftest compile-derived-stage-filter-on-aggregation-test
  (with-fixture
    (testing "drilling on an aggregation value (filter references it by Lib's name) is applied"
      (let [card {:source-table 100 :aggregation [[:count]] :breakout [[:field 2 nil]]}
            {:keys [sparql vars]}
            (compile-stage* {:source-query card
                             :filter [:< [:field "count" nil] 12]})]
        (is (= ["naam" "ag_0"] vars))
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
                  :breakout    [[:field 10 {:join-alias "Plaats"}]]
                  :joins       [{:alias "Plaats" :fk-field-id 4}]}
            ;; Lib names the joined breakout column "Geboorteplaats" — different from the
            ;; driver's invented SPARQL var "Plaats__label".
            expected [{:name "Geboorteplaats"} {:name "count"}]
            {:keys [sparql vars]}
            (@#'mbql/compile-derived-stage
             {:source-query card
              :filter [:= [:field "Geboorteplaats" nil] "Leuven"]}
             expected)]
        (is (= ["Plaats__label" "ag_0"] vars))
        (is (str/includes? sparql "FILTER (?Plaats__label = \"Leuven\")")
            "the Lib column name resolves to the joined SPARQL variable")))
    (testing "the same name-aliasing applies to order-by on a joined breakout column"
      (let [card {:source-table 100
                  :aggregation [[:count]]
                  :breakout    [[:field 10 {:join-alias "Plaats"}]]
                  :joins       [{:alias "Plaats" :fk-field-id 4}]}
            expected [{:name "Geboorteplaats"} {:name "count"}]
            {:keys [sparql]}
            (@#'mbql/compile-derived-stage
             {:source-query card
              :order-by [[:asc [:field "Geboorteplaats" nil]]]}
             expected)]
        (is (str/includes? sparql "ORDER BY ASC(?Plaats__label)"))))))

(deftest compile-derived-stage-outer-count-test
  (with-fixture
    (testing "an arg-less count on a derived stage uses COUNT(*) (no ?subject available)"
      (let [card {:source-table 100 :aggregation [[:count]] :breakout [[:field 2 nil]]}
            {:keys [sparql vars]}
            (compile-stage* {:source-query card
                             :aggregation [[:count]]
                             :breakout [[:field "naam" nil]]})]
        (is (= ["naam" "ag_0"] vars))
        (is (str/includes? sparql "(COUNT(*) AS ?ag_0)"))
        (is (str/includes? sparql "GROUP BY ?naam"))))))

;; ---------------------------------------------------------------------------
;; Lib-driven projection (the column-count-mismatch fix)
;; ---------------------------------------------------------------------------

(deftest reconcile-base-projection-test
  (with-fixture
    (let [f   @#'mbql/reconcile-base-projection
          ctx {:field-id->var           {2 "naam" 3 "leeftijd"}
               :pair->target-var        {[10 "Plaats"] "Plaats__label"}
               :alias->intermediate-var {"Plaats" "Plaats_subject"}
               :default-graph           base}]
      (testing "columns the compiler already projects reuse their variable"
        (is (= {:vars ["subject" "naam" "Plaats__label"] :triples []}
               (f [{:id 1} {:id 2} {:id 10 :lib/join-alias "Plaats"}] ctx))))
      (testing "a joined column the compiler missed is synthesized off the intermediate var"
        (is (= {:vars    ["subject" "Plaats__naam"]
                :triples [(str "  OPTIONAL { ?Plaats_subject <" base "naam> ?Plaats__naam . }")]}
               (f [{:id 1} {:id 2 :lib/join-alias "Plaats"}] ctx))))
      (testing "an unresolvable column still gets a (placeholder) variable"
        (is (= {:vars ["undefined_1"] :triples []}
               (f [{:lib/join-alias "Nope"}] ctx)))))))

(deftest compile-base-stage-lib-driven-projection-test
  (with-fixture
    (testing "the SELECT is reconciled against Lib's expected columns"
      (let [stage    {:source-table 100
                      :fields [[:field 1 nil] [:field 2 nil]
                               [:field 10 {:join-alias "Plaats"}]]
                      :joins  [{:alias "Plaats" :fk-field-id 4}]}
            ;; Lib expects an extra joined column (id 3) the :fields list omits —
            ;; the FK-remap-on-a-remap case behind the recurring column mismatch.
            expected [{:id 1} {:id 2}
                      {:id 10 :lib/join-alias "Plaats"}
                      {:id 3  :lib/join-alias "Plaats"}]
            {:keys [sparql vars]} (@#'mbql/compile-base-stage stage expected)]
        (is (= 4 (count vars)) "one SELECT variable per Lib expected column")
        (is (= ["subject" "naam" "Plaats__label" "Plaats__leeftijd"] vars))
        (is (str/includes? sparql "SELECT ?subject ?naam ?Plaats__label ?Plaats__leeftijd"))
        (testing "the missing column is synthesized off the join's intermediate var"
          (is (str/includes?
               sparql
               (str "OPTIONAL { ?Plaats_subject <" base "leeftijd> ?Plaats__leeftijd . }"))))))))

(deftest compile-base-stage-lib-driven-order-test
  (with-fixture
    (testing "expected-cols drives column order, independent of :fields order"
      (let [stage {:source-table 100
                   :fields [[:field 1 nil] [:field 2 nil] [:field 3 nil]]}
            {:keys [vars]} (@#'mbql/compile-base-stage stage [{:id 1} {:id 3} {:id 2}])]
        (is (= ["subject" "leeftijd" "naam"] vars))))))

(deftest compile-derived-stage-lib-driven-projection-test
  (with-fixture
    (testing "expected-cols drives the derived-stage SELECT and preserves the column count"
      (let [card {:source-table 100 :aggregation [[:count]] :breakout [[:field 2 nil]]}
            ;; the inner card projects [naam ag_0]; Lib expects a third column
            {:keys [vars]} (@#'mbql/compile-derived-stage
                            {:source-query card}
                            [{:id 2} {:id 3} {:id 99 :lib/join-alias "Missing"}])]
        (is (= 3 (count vars)))
        (is (= ["naam" "ag_0"] (take 2 vars)))
        (is (str/starts-with? (last vars) "undefined_"))))))

(deftest compile-base-stage-lang-filter-test
  (testing "rdf:langString columns get a LANG filter when a default language is set"
    (with-redefs-fn
      {#'mbql/field-id->metadata        (fn [id] (get {1 {:name "subject"}
                                                       2 {:name "naam" :database-type "langString"}}
                                                      id))
       #'mbql/table-id->class-uri       (constantly (str base "Persoon"))
       #'mbql/database-default-graph    (constantly base)
       #'mbql/database-default-language (constantly "nl")}
      (fn []
        (let [{:keys [sparql]}
              (compile-stage* {:source-table 100
                               :fields [[:field 1 nil] [:field 2 nil]]})]
          (is (str/includes?
               sparql
               "FILTER(!BOUND(?naam) || LANG(?naam) = \"nl\" || LANG(?naam) = \"\")")))))))

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
        (is (str/includes? sparql "FILTER (STR(?lokatie) = \"POINT(4.70 50.88)\")"))
        (is (not (str/includes? sparql "(?lokatie = ")))))
    (testing "!= on a geometry field also uses STR()"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 11 nil]]
                             :filter [:!= [:field 11 nil] "POINT(4.70 50.88)"]})]
        (is (str/includes? sparql "FILTER (STR(?lokatie) != \"POINT(4.70 50.88)\")"))))
    (testing "a geometry equality is NOT pushed into a mandatory BGP anchor triple"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 11 nil]]
                             :filter [:= [:field 11 nil] "POINT(4.70 50.88)"]})]
        (is (not (str/includes? sparql "?subject <https://odis.q.libis.be/lokatie> \"POINT")))))))

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
                             :fields [[:field 1 nil] [:field 2 nil] [:expression "upper_naam"]]
                             :expressions {"upper_naam" [:upper [:field 2 nil]]}})]
        (is (some #{"upper_naam"} vars))
        (is (str/includes? sparql "BIND(UCASE(STR(?naam)) AS ?upper_naam)"))
        (is (str/includes? sparql "SELECT ?subject ?naam ?upper_naam"))))
    (testing "a field referenced only inside an expression still gets its triple"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:expression "len"]]
                             :expressions {"len" [:length [:field 3 nil]]}})]
        (is (str/includes? sparql "OPTIONAL { ?subject <https://odis.q.libis.be/leeftijd> ?leeftijd . }"))
        (is (str/includes? sparql "BIND(STRLEN(STR(?leeftijd)) AS ?len)"))))
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
        (is (str/includes? sparql "OPTIONAL { ?subject <https://odis.q.libis.be/lokatie> ?lokatie . }"))
        (is (str/includes?
             sparql
             "BIND(<http://www.w3.org/2001/XMLSchema#double>(REPLACE(STR(?lokatie), \"^.*?([-0-9.]+).*$\", \"$1\")) AS ?lon)"))
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
      (is (= {:axis "point-lon" :source "lokatie"} (marker 12)))
      (is (= {:axis "box-min-lon" :source "begrenzingsvak"} (marker 14)))
      (is (geo? 12))
      (is (not (geo? 11)))   ;; raw geometry column is not itself a coordinate
      (is (not (geo? 2))))))

(deftest geo-coord-point-compile-test
  (with-fixture
    (testing "selecting synthesized lon/lat emits ONE source triple + two extraction BINDs"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 12 nil] [:field 13 nil]]})]
        (is (= ["subject" "lokatie_lon" "lokatie_lat"] vars))
        (is (str/includes? sparql "OPTIONAL { ?subject <https://odis.q.libis.be/lokatie> ?lokatie . }"))
        (is (str/includes?
             sparql
             "BIND(<http://www.w3.org/2001/XMLSchema#double>(REPLACE(STR(?lokatie), "))
        (is (str/includes? sparql "POINT"))
        (is (str/includes? sparql "\"$1\")) AS ?lokatie_lon)"))
        (is (str/includes? sparql "\"$2\")) AS ?lokatie_lat)"))
        (testing "no bogus property triple for the coordinate columns themselves"
          (is (not (str/includes? sparql "lokatie_lon>")))
          (is (not (str/includes? sparql "lokatie_lat>"))))))
    (testing "the source geometry triple is shared (emitted once) for lon+lat"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 12 nil] [:field 13 nil]]})]
        (is (= 1 (count (re-seq #"<https://odis\.q\.libis\.be/lokatie> \?lokatie " sparql))))))))

(deftest geo-coord-box-compile-test
  (with-fixture
    (testing "BOX corners extract via a 4-group REPLACE off the begrenzingsvak source"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 14 nil] [:field 15 nil]]})]
        (is (= ["subject" "begrenzingsvak_min_lon" "begrenzingsvak_max_lat"] vars))
        (is (str/includes? sparql "<https://odis.q.libis.be/begrenzingsvak> ?begrenzingsvak"))
        (is (str/includes? sparql "BOX"))
        (is (str/includes? sparql "\"$1\")) AS ?begrenzingsvak_min_lon)"))
        (is (str/includes? sparql "\"$4\")) AS ?begrenzingsvak_max_lat)"))))))

(deftest geo-coord-joined-compile-test
  (with-fixture
    (testing "a synthesized coordinate reached via an implicit join binds off the join var"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 12 {:join-alias "Plaats"}]]
                             :joins  [{:alias "Plaats" :fk-field-id 4}]})]
        ;; FK + joined source geometry triple, then the BIND off the joined source var
        (is (str/includes? sparql "OPTIONAL { ?subject <https://odis.q.libis.be/geboorteplaats> ?Plaats_subject . }"))
        (is (str/includes? sparql "OPTIONAL { ?Plaats_subject <https://odis.q.libis.be/lokatie> ?Plaats__lokatie . }"))
        (is (str/includes? sparql "REPLACE(STR(?Plaats__lokatie), "))
        (is (str/includes? sparql "AS ?Plaats__lokatie_lon)"))))))

(deftest geo-coord-filter-test
  (with-fixture
    (testing "a synthesized coordinate can be filtered numerically (and its BIND is emitted)"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 12 nil]]
                             :filter [:< [:field 12 nil] 5]})]
        (is (str/includes? sparql "AS ?lokatie_lon)"))
        (is (str/includes? sparql "FILTER (?lokatie_lon < 5)"))))))

(deftest inside-and-between-filter-test
  (with-fixture
    (testing ":between compiles to a numeric range"
      (let [f #(@#'mbql/compile-filter-expr % {"leeftijd" "leeftijd"} {})]
        (is (= "(?leeftijd >= 18 && ?leeftijd <= 65)"
               (f [:between [:field "leeftijd" nil] 18 65])))
        (testing "a [:value ...] wrapped bound is unwrapped"
          (is (= "(?leeftijd >= 18 && ?leeftijd <= 65)"
                 (f [:between [:field "leeftijd" nil] [:value 18 {}] [:value 65 {}]]))))))
    (testing ":inside compiles to a bounding box over lat/lon (N/W/S/E order)"
      (let [f #(@#'mbql/compile-filter-expr % {"lat" "lat" "lon" "lon"} {})]
        (is (= "((?lat <= 51.0) && (?lat >= 50.0) && (?lon >= 4.0) && (?lon <= 5.0))"
               (f [:inside [:field "lat" nil] [:field "lon" nil] 51.0 4.0 50.0 5.0])))))
    (testing ":inside on synthesized coordinate columns emits their BINDs + the box FILTER"
      (let [{:keys [sparql]}
            (compile-stage* {:source-table 100
                             :fields [[:field 1 nil] [:field 12 nil] [:field 13 nil]]
                             :filter [:inside [:field 13 nil] [:field 12 nil] 51.0 4.0 50.0 5.0]})]
        (is (str/includes? sparql "OPTIONAL { ?subject <https://odis.q.libis.be/lokatie> ?lokatie . }"))
        (is (str/includes? sparql "AS ?lokatie_lon)"))
        (is (str/includes? sparql "AS ?lokatie_lat)"))
        (is (str/includes?
             sparql
             "FILTER ((?lokatie_lat <= 51.0) && (?lokatie_lat >= 50.0) && (?lokatie_lon >= 4.0) && (?lokatie_lon <= 5.0))"))))))

;; ---------------------------------------------------------------------------
;; Binning (grid / heat maps, numeric bins)
;; ---------------------------------------------------------------------------

(deftest bin-expr-test
  (let [f @#'mbql/bin-expr]
    (testing "anchored at zero drops the min offset"
      (is (= "(FLOOR(?lon / 0.1) * 0.1)" (f "lon" 0.1 0))))
    (testing "non-zero min uses the full floor formula"
      (is (= "((FLOOR((?leeftijd - 10) / 10) * 10) + 10)" (f "leeftijd" 10 10))))))

(deftest binned-breakout-compile-test
  (with-fixture
    (testing "a binned numeric breakout buckets the column and groups by the bucket var"
      (let [{:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :breakout    [[:field 3 {:binning {:strategy :bin-width :bin-width 10 :min-value 0}}]]
                             :aggregation [[:count]]})]
        (is (= ["leeftijd_binned" "ag_0"] vars))
        (is (str/includes? sparql "OPTIONAL { ?subject <https://odis.q.libis.be/leeftijd> ?leeftijd . }"))
        (is (str/includes? sparql "BIND((FLOOR(?leeftijd / 10) * 10) AS ?leeftijd_binned)"))
        (is (str/includes? sparql "GROUP BY ?leeftijd_binned"))
        (is (str/includes? sparql "(COUNT(DISTINCT ?subject) AS ?ag_0)"))))))

(deftest grid-map-binned-coordinates-test
  (with-fixture
    (testing "grid map: binned synthesized lon/lat + count -> coordinate BINDs, bin BINDs, grouped"
      (let [bin {:strategy :bin-width :bin-width 0.1 :min-value 0}
            {:keys [sparql vars]}
            (compile-stage* {:source-table 100
                             :breakout    [[:field 12 {:binning bin}] [:field 13 {:binning bin}]]
                             :aggregation [[:count]]})]
        (is (= ["lokatie_lon_binned" "lokatie_lat_binned" "ag_0"] vars))
        ;; one shared source geometry triple
        (is (str/includes? sparql "OPTIONAL { ?subject <https://odis.q.libis.be/lokatie> ?lokatie . }"))
        ;; coordinate extraction BINDs feed the bin BINDs
        (is (str/includes? sparql "AS ?lokatie_lon)"))
        (is (str/includes? sparql "AS ?lokatie_lat)"))
        (is (str/includes? sparql "BIND((FLOOR(?lokatie_lon / 0.1) * 0.1) AS ?lokatie_lon_binned)"))
        (is (str/includes? sparql "BIND((FLOOR(?lokatie_lat / 0.1) * 0.1) AS ?lokatie_lat_binned)"))
        (is (str/includes? sparql "GROUP BY ?lokatie_lon_binned ?lokatie_lat_binned"))))))
