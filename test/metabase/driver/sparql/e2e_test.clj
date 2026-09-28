(ns metabase.driver.sparql.e2e-test
  "End-to-end tests: real pMBQL queries compiled the way the QP does
  (preprocessing, then driver/mbql->native) and run through
  driver/execute-reducible-query against the live SPARQL endpoint from the
  smoke harness (make smoke). Covers what the unit tests cannot: that the
  generated SPARQL behaves as intended on a real engine, the QP-store detail
  lookup, and conversion of live results into rows and column metadata."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [metabase.driver-api.core :as driver-api]
            [metabase.driver.sparql.database :as database]
            [metabase.driver.sparql.mbql :as mbql]
            [metabase.driver.sparql.test-util :as tu]
            [metabase.lib.core :as lib]))

(use-fixtures :once tu/skip-without-live-endpoint)

(deftest ^:integration provider-matches-live-schema-test
  (testing "every provider column exists in describe-table's live view of the
            fixture, so the other tests exercise sync-discoverable fields
            (the provider is a deliberate subset: sync also finds rdf:type)"
    (let [live-names (->> (database/describe-table :sparql tu/db {:name "Person"})
                          :fields
                          (map :name)
                          set)]
      (is (set/subset? (set (map :name (lib/fieldable-columns (tu/person-query))))
                       live-names)))))

(deftest ^:integration fields-projection-rows-and-types-test
  (let [q     (tu/person-query)
        label (tu/column q tu/rdfs-label)
        age   (tu/column q "age")
        {:keys [cols rows]} (tu/run-query (lib/with-fields q [label age]))]
    ;; exactly 2 columns proves returned-columns drove the projection: the
    ;; compiler's fallback path would project ?subject as well.
    (is (= 2 (count cols)))
    (is (= [:type/Text :type/Integer] (mapv :base_type cols)))
    (is (= #{["Alice" 30] ["Bob" 25]} (set rows)))
    (is (every? #(instance? Long (second %)) rows))))

(deftest ^:integration default-projection-includes-subject-test
  (let [{:keys [cols rows]} (tu/run-query (tu/person-query))]
    (is (= 7 (count cols)))
    (is (= :type/URL (:base_type (first cols))))
    (is (= #{"https://example.org/alice" "https://example.org/bob"}
           (set (map first rows))))))

(deftest ^:integration filter-equals-label-test
  (let [q     (tu/person-query)
        label (tu/column q tu/rdfs-label)
        q     (-> q
                  (lib/with-fields [label])
                  (lib/filter (lib/= label "Alice")))
        {:keys [rows]} (tu/run-query q)]
    (is (= [["Alice"]] rows))))

(deftest ^:integration filter-equals-language-tagged-literal-test
  (testing "a picked value matches the language-tagged literal with that text"
    (let [q        (tu/person-query)
          label    (tu/column q tu/rdfs-label)
          nickname (tu/column q "nickname")
          rows     #(:rows (tu/run-query (-> q (lib/with-fields [label]) (lib/filter %))))]
      (is (= [["Alice"]] (rows (lib/= nickname "Ally"))))
      (is (= [["Alice"]] (rows (lib/!= nickname "Bobby")))
          "is not also excludes the tagged match"))))

(deftest ^:integration default-language-keeps-rows-without-that-language-test
  (testing "with Default Language en, Bob (only a @fr nickname) keeps his row, nickname empty"
    (with-redefs [mbql/database-default-language (constantly "en")]
      (let [q        (tu/person-query)
            label    (tu/column q tu/rdfs-label)
            nickname (tu/column q "nickname")]
        (is (= #{["Alice" "Ally"] ["Bob" nil]}
               (set (:rows (tu/run-query (lib/with-fields q [label nickname]))))))))))

(deftest ^:integration iri-equality-filter-test
  (testing "equality on the subject column matches the IRI node"
    (let [q     (tu/person-query)
          label (tu/column q tu/rdfs-label)
          subj  (tu/column q "subject")
          q     (-> q
                    (lib/with-fields [label])
                    (lib/filter (lib/= subj "https://example.org/alice")))
          {:keys [rows]} (tu/run-query q)]
      (is (= [["Alice"]] rows))))
  (testing "equality on an IRI-valued property (knows, database-type \"uri\") matches the node"
    (let [q     (tu/person-query)
          label (tu/column q tu/rdfs-label)
          knows (tu/column q "knows")
          q     (-> q
                    (lib/with-fields [label])
                    (lib/filter (lib/= knows "https://example.org/bob")))
          {:keys [rows]} (tu/run-query q)]
      (is (= [["Alice"]] rows)))))

(deftest ^:integration negated-filters-keep-rows-without-the-value-test
  (let [q     (tu/person-query)
        label (tu/column q tu/rdfs-label)
        knows (tu/column q "knows")
        rows  #(set (:rows (tu/run-query (-> q (lib/with-fields [label]) (lib/filter %)))))]
    (testing "is not: Bob knows nobody, so he is not someone who knows Bob"
      (is (= #{["Bob"]} (rows (lib/!= knows "https://example.org/bob")))))
    (testing "does not contain: Bob has no knows value, so it does not contain \"bob\""
      (is (= #{["Bob"]} (rows (lib/does-not-contain knows "bob")))))))

(deftest ^:integration filter-between-age-test
  (let [q     (tu/person-query)
        label (tu/column q tu/rdfs-label)
        age   (tu/column q "age")
        q     (-> q
                  (lib/with-fields [label age])
                  (lib/filter (lib/between age 26 35)))
        {:keys [rows]} (tu/run-query q)]
    (is (= [["Alice" 30]] rows))))

(deftest ^:integration date-filter-test
  (testing "a date range from the UI matches typed xsd:date values"
    (let [q     (tu/person-query)
          label (tu/column q tu/rdfs-label)
          born  (tu/column q "birthDate")
          {:keys [rows]} (tu/run-query (-> q
                                           (lib/with-fields [label])
                                           (lib/filter (lib/between born "1990-01-01" "1999-12-31"))))]
      (is (= [["Alice"]] rows)))))

(deftest ^:integration datetime-filter-mixed-timezones-test
  ;; Alice's value carries a timezone, Bob's does not. XSD comparisons between
  ;; the two forms are indeterminate, so a single bound would drop Bob here.
  (let [q     (tu/person-query)
        label (tu/column q tu/rdfs-label)
        upd   (tu/column q "updated")
        run   #(:rows (tu/run-query (-> q (lib/with-fields [label]) (lib/filter %))))]
    (testing "a timezone-less value is compared against the local bound"
      (is (= [["Bob"]] (run (lib/< upd "2024-01-01")))))
    (testing "a timezoned value is compared against the offset bound"
      (is (= [["Alice"]] (run (lib/>= upd "2024-01-01")))))))

(deftest ^:integration relative-date-filter-test
  (testing "a relative preset (desugared by the QP) runs on the endpoint"
    (let [q     (tu/person-query)
          label (tu/column q tu/rdfs-label)
          born  (tu/column q "birthDate")
          {:keys [rows]} (tu/run-query (-> q
                                           (lib/with-fields [label])
                                           (lib/filter (lib/time-interval born -200 :year))))]
      (is (= #{["Alice"] ["Bob"]} (set rows))))))

(deftest ^:integration order-by-limit-test
  (let [q     (tu/person-query)
        label (tu/column q tu/rdfs-label)
        age   (tu/column q "age")
        q     (-> q
                  (lib/with-fields [label age])
                  (lib/order-by age :desc)
                  (lib/limit 1))
        {:keys [rows]} (tu/run-query q)]
    (is (= [["Alice" 30]] rows))))

(deftest ^:integration custom-columns-test
  (let [q     (tu/person-query)
        label (tu/column q tu/rdfs-label)
        age   (tu/column q "age")
        q     (-> q
                  (lib/with-fields [label])
                  (lib/expression "shout" (lib/upper label))
                  (lib/expression "doubled" (lib/* age 2))
                  ;; no default and no match: both are null for Bob
                  (lib/expression "older" (lib/case [[(lib/> age 26) "old"]]))
                  (lib/expression "li" (lib/regex-match-first label "l."))
                  ;; 30/7 = 4.29 and 25/7 = 3.57: Bob gets 4 only if it rounds
                  (lib/expression "sevenths" (lib/integer (lib// age 7)))
                  ;; null, as in SQL, where Virtuoso would fail the whole query
                  (lib/expression "by-zero" (lib// age 0)))]
    (testing "custom columns come back computed, with null where Metabase gives null"
      (is (= #{["Alice" "ALICE" 60 "old" "li" 4 nil] ["Bob" "BOB" 50 nil nil 4 nil]}
             (set (:rows (tu/run-query q))))))
    (testing "is-null on a custom column matches its null rows"
      (is (= [["Bob"]]
             (->> (lib/filter q (lib/is-null (lib/expression-ref q "older")))
                  tu/run-query
                  :rows
                  (map (partial take 1))))))))

(deftest ^:integration count-aggregation-test
  (let [q (lib/aggregate (tu/person-query) (lib/count))
        {:keys [cols rows]} (tu/run-query q)]
    (is (= [:type/Integer] (mapv :base_type cols)))
    (is (= [[2]] rows))
    (is (instance? Long (ffirst rows)))))

(deftest ^:integration count-by-breakout-test
  (let [q     (tu/person-query)
        label (tu/column q lib/breakoutable-columns tu/rdfs-label)
        q     (-> q
                  (lib/breakout label)
                  (lib/aggregate (lib/count)))
        {:keys [rows]} (tu/run-query q)]
    (is (= #{["Alice" 1] ["Bob" 1]} (set rows)))))

(deftest ^:integration temporal-breakout-test
  (testing "grouping a date by year returns the start of each year"
    (let [q    (tu/person-query)
          born (tu/column q lib/breakoutable-columns "birthDate")
          {:keys [rows]} (tu/run-query (-> q
                                           (lib/breakout (lib/with-temporal-bucket born :year))
                                           (lib/aggregate (lib/count))))]
      (is (= #{["1994-01-01" 1] ["2001-01-01" 1]}
             (set (map (fn [[d n]] [(str d) n]) rows)))))))

(deftest ^:integration derived-stage-aggregation-filter-test
  ;; drill-through shape: an outer stage filtering the inner stage's
  ;; aggregation result by its Lib name (resolved via expected-name->var).
  (let [base      (let [q (tu/person-query)]
                    (-> q
                        (lib/breakout (tu/column q lib/breakoutable-columns tu/rdfs-label))
                        (lib/aggregate (lib/count))
                        (lib/append-stage)))
        count-col (tu/column base lib/filterable-columns "count")]
    (testing "filter that keeps every group"
      (let [{:keys [rows]} (tu/run-query (lib/filter base (lib/> count-col 0)))]
        (is (= #{["Alice" 1] ["Bob" 1]} (set rows)))))
    (testing "filter that excludes every group"
      (let [{:keys [cols rows]} (tu/run-query (lib/filter base (lib/> count-col 5)))]
        ;; a successful empty result still carries the SELECT vars as columns
        ;; (the error path throws instead of responding) — don't pass on failure.
        (is (= 2 (count cols)))
        (is (= [] rows))))))

(deftest ^:integration native-join-aggregation-test
  (testing "a native query joining Company to City aggregates per group"
    (let [{:keys [rows]} (tu/run-native
                          "PREFIX ex: <https://example.org/>
                           PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                           SELECT ?city (COUNT(?c) AS ?companies) (SUM(?emp) AS ?employees)
                           WHERE { ?c a ex:Company ; ex:employees ?emp ; ex:headquarters ?hq .
                                   ?hq rdfs:label ?city }
                           GROUP BY ?city ORDER BY ?city")]
      (is (= [["Shelbyville" 1 300] ["Springfield" 2 165]] rows)))))

(deftest ^:integration native-ask-returns-boolean-test
  (let [{:keys [cols rows]} (tu/run-native "ASK { ?s a <https://example.org/Person> }")]
    (is (= [{:name "boolean" :display_name "boolean" :base_type :type/Boolean}]
           cols))
    (is (= [[true]] rows))))

(deftest ^:integration native-syntax-error-raises-test
  (testing "a malformed native query raises an invalid-query error carrying the
            endpoint's message, instead of succeeding with an empty result"
    ;; Premise: the harness endpoint (Oxigraph) answers a SPARQL parse error
    ;; with HTTP 400, which classifies as :query → invalid-query. An engine
    ;; that answered 200 + an error JSON would take the success branch instead;
    ;; revisit this assertion if the smoke harness ever changes engines.
    (let [e (is (thrown? clojure.lang.ExceptionInfo
                         (tu/run-native "SELECT ?x WHERE {")))]
      (is (= driver-api/qp.error-type.invalid-query (:type (ex-data e))))
      (is (str/includes? (ex-message e) "Error executing SPARQL query")))))
