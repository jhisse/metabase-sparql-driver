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
             (f [:or [:= [:field "naam" nil] "Jan"] [:= [:field "naam" nil] "Piet"]]))))))

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
   10 {:name "label" :database-type "string"}})

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
    (let [{:keys [sparql]}
          (compile-stage* {:source-table 100
                           :fields [[:field 1 nil] [:field 2 nil]]
                           :filter [:= [:field 2 nil] "Jan"]})]
      (is (str/includes? sparql "FILTER (?naam = \"Jan\")")))))

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
