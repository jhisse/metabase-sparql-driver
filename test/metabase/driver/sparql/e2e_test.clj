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
            [metabase.lib.core :as lib]
            [metabase.lib.metadata :as lib.metadata]))

(use-fixtures :once tu/skip-without-live-endpoint)

(deftest ^:integration provider-matches-live-schema-test
  (testing "every provider column exists in describe-table's live view of the
            fixture, so the other tests exercise sync-discoverable fields
            (the provider is a deliberate subset: sync also finds rdf:type)"
    (doseq [[table-name q] {"Person"  (tu/person-query)
                            "Company" (tu/company-query)
                            "City"    (tu/city-query)}]
      (testing table-name
        (let [live-names (->> (database/describe-table :sparql tu/db {:name table-name})
                              :fields
                              (map :name)
                              set)]
          (is (set/subset? (set (map :name (lib/fieldable-columns q)))
                           live-names)))))))

(defn- via-fk
  "Return the column named `col-name` that (columns-fn q) reaches through the
  FK column `fk-name` (an implicit join). Throws when absent, like [[tu/column]]."
  [q columns-fn fk-name col-name]
  (let [fk-id (:id (tu/column q fk-name))]
    (or (first (filter #(and (= col-name (:name %)) (= fk-id (:fk-field-id %)))
                       (columns-fn q)))
        (throw (ex-info (str "No column named " col-name " via " fk-name) {})))))

(defn- left-join-company
  "Return `q` with only its label, left-joined as \"C\" to every Company field
  on `worksFor` = Company's subject."
  [q]
  (let [company (tu/company-query)]
    (-> q
        (lib/with-fields [(tu/column q tu/rdfs-label)])
        (lib/join (-> (lib/join-clause (lib.metadata/table company (lib/primary-source-table-id company))
                                       [(lib/= (tu/column q "worksFor")
                                               (lib/with-join-alias (tu/column company "subject") "C"))])
                      (lib/with-join-alias "C")
                      (lib/with-join-fields :all))))))

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
    ;; 8 fields, then the display values of the knows and worksFor FKs
    (is (= 10 (count cols)))
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

(deftest ^:integration case-skips-a-when-on-a-missing-value-test
  (testing "Bob has no knows: the first WHEN is unknown, so the second one decides, as in SQL"
    (let [q     (tu/person-query)
          label (tu/column q tu/rdfs-label)
          knows (tu/column q "knows")
          q     (-> q
                    (lib/with-fields [label])
                    (lib/expression "tag" (lib/case [[(lib/contains knows "alice") "knows alice"]
                                                     [(lib/= label "Bob") "is bob"]])))]
      (is (= #{["Alice" nil] ["Bob" "is bob"]} (set (:rows (tu/run-query q))))))))

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

(deftest ^:integration page-test
  (testing "each page holds the next rows (the agent API paginates with :page)"
    (let [q    (tu/person-query)
          age  (tu/column q "age")
          q    (-> q
                   (lib/with-fields [(tu/column q tu/rdfs-label)])
                   (lib/order-by age :desc))
          page #(:rows (tu/run-query (lib/with-page q {:page % :items 1})))]
      (is (= [["Alice"]] (page 1)))
      (is (= [["Bob"]] (page 2)))
      (is (= [] (page 3))))))

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

(deftest ^:integration property-named-subject-test
  (testing "a property whose short name is `subject` keeps its full IRI, apart from the key"
    (let [fields (:fields (database/describe-table :sparql tu/db {:name "City"}))
          named  (fn [nm] (filter #(= nm (:name %)) fields))]
      (is (= ["uri"] (map :database-type (named "subject"))))
      (is (= 1 (count (named (str tu/ex "subject")))))))
  (testing "and its values come back under that column"
    (let [q    (tu/city-query)
          prop (tu/column q (str tu/ex "subject"))
          {:keys [rows]} (tu/run-query (lib/with-fields q [(tu/column q tu/rdfs-label) prop]))]
      (is (= {["Shelbyville" nil] 1 ["Springfield" "Simpsons"] 1} (frequencies rows))))))

(deftest ^:integration aggregations-test
  (testing "sum, average, min and max of age"
    (let [q   (tu/person-query)
          age (tu/column q "age")
          q   (-> q
                  (lib/aggregate (lib/sum age))
                  (lib/aggregate (lib/avg age))
                  (lib/aggregate (lib/min age))
                  (lib/aggregate (lib/max age)))]
      (is (= [[55 27.5 25 30]] (:rows (tu/run-query q))))))
  (testing "distinct headquarters against the count of companies (three companies, two cities)"
    (let [q (tu/company-query)
          q (-> q
                (lib/aggregate (lib/distinct (tu/column q "headquarters")))
                (lib/aggregate (lib/count)))]
      (is (= [[2 3]] (:rows (tu/run-query q))))))
  (testing "count of companies by a boolean"
    (let [q (tu/company-query)
          q (-> q
                (lib/breakout (tu/column q lib/breakoutable-columns "listed"))
                (lib/aggregate (lib/count)))]
      (is (= #{[false 1] [true 2]} (set (:rows (tu/run-query q))))))))

(deftest ^:integration string-and-empty-filters-test
  (let [q     (tu/person-query)
        label (tu/column q tu/rdfs-label)
        knows (tu/column q "knows")
        rows  #(set (:rows (tu/run-query (-> q (lib/with-fields [label]) (lib/filter %)))))]
    (testing "contains and starts with, ignoring case"
      (is (= #{["Alice"]} (rows (lib/ignore-case (lib/contains label "LIC")))))
      (is (= #{["Bob"]} (rows (lib/ignore-case (lib/starts-with label "b"))))))
    (testing "is empty and not empty on an IRI-valued property"
      (is (= #{["Bob"]} (rows (lib/is-empty knows))))
      (is (= #{["Alice"]} (rows (lib/not-empty knows)))))))

(deftest ^:integration relative-date-filter-starting-from-test
  ;; The first window spans 150 years back to 50 years ahead and the second
  ;; ends 200 years ago, so both birth dates (1994, 2001) stay inside the first
  ;; and outside the second until at least the 2140s; without the offset, each
  ;; window would give the other answer.
  (let [q     (tu/person-query)
        label (tu/column q tu/rdfs-label)
        born  (tu/column q "birthDate")
        rows  #(set (:rows (tu/run-query (-> q (lib/with-fields [label]) (lib/filter %)))))]
    (testing "the next 200 years, starting 150 years ago"
      (is (= #{["Alice"] ["Bob"]} (rows (lib/relative-time-interval born 200 :year -150 :year)))))
    (testing "the previous 200 years, starting 200 years ago"
      (is (= #{} (rows (lib/relative-time-interval born -200 :year -200 :year)))))))

(deftest ^:integration text-custom-columns-test
  (let [q     (tu/person-query)
        label (tu/column q tu/rdfs-label)
        q     (-> q
                  (lib/with-fields [label])
                  (lib/expression "greeting" (lib/concat "Hi " label))
                  (lib/expression "same" (lib/replace label "" "x"))
                  (lib/expression "friend" (lib/coalesce (tu/column q "knows") "nobody"))
                  (lib/expression "chars" (lib/length label)))]
    (is (= #{["Alice" "Hi Alice" "Alice" (str tu/ex "bob") 5]
             ["Bob" "Hi Bob" "Bob" "nobody" 3]}
           (set (:rows (tu/run-query q)))))))

(deftest ^:integration implicit-fk-join-test
  (testing "count of people by their employer's label"
    (let [q (tu/person-query)
          q (-> q
                (lib/breakout (via-fk q lib/breakoutable-columns "worksFor" tu/rdfs-label))
                (lib/aggregate (lib/count)))]
      (is (= #{["Acme Inc" 1] ["Globex" 1]} (set (:rows (tu/run-query q)))))))
  (testing "count of companies by their headquarters' label"
    (let [q (tu/company-query)
          q (-> q
                (lib/breakout (via-fk q lib/breakoutable-columns "headquarters" tu/rdfs-label))
                (lib/aggregate (lib/count)))]
      (is (= #{["Springfield" 2] ["Shelbyville" 1]} (set (:rows (tu/run-query q)))))))
  (testing "a FK column comes back with its display value"
    (let [q (tu/person-query)
          q (lib/with-fields q [(tu/column q tu/rdfs-label) (tu/column q "worksFor")])]
      (is (= (frequencies [["Alice" (str tu/ex "acme") "Acme Inc"]
                           ["Bob" (str tu/ex "globex") "Globex"]])
             (frequencies (:rows (tu/run-query q))))))))

(def ^:private label-var
  "SPARQL variable, and column name, of the rdfs:label column."
  "http___www_w3_org_2000_01_rdf_schema_label")

(defn- column-values
  "Return how many times each row of `result` occurs, each row cut down to the
  columns named `col-names`."
  [{:keys [cols rows]} col-names]
  (let [index (zipmap (map :name cols) (range))]
    (frequencies (map (fn [row] (mapv #(nth row (index %)) col-names)) rows))))

(def ^:private company-join-rows
  "Each person's label, the joined company's subject and label, and the label
  of that company's headquarters (its FK display value)."
  (frequencies [["Alice" (str tu/ex "acme") "Acme Inc" "Springfield"]
                ["Bob" (str tu/ex "globex") "Globex" "Springfield"]]))

(deftest ^:integration explicit-left-join-test
  (testing "the joined subject, its label, and the display value of its own FK one hop further"
    (is (= company-join-rows
           (column-values (tu/run-query (left-join-company (tu/person-query)))
                          [label-var "C_subject" (str "C__" label-var) (str "C__headquarters__" label-var)]))))
  (testing "a self-join on knows: each FK display value is read off its own row's node"
    (let [q      (tu/person-query)
          people (tu/person-query)
          q      (-> q
                     (lib/with-fields [(tu/column q tu/rdfs-label) (tu/column q "worksFor")])
                     (lib/join (-> (lib/join-clause (lib.metadata/table q (lib/primary-source-table-id q))
                                                    [(lib/= (tu/column q "knows")
                                                            (lib/with-join-alias (tu/column people "subject") "P"))])
                                   (lib/with-join-alias "P")
                                   (lib/with-join-fields :all))))]
      ;; own label, own employer's label, the known person's label and employer's label
      (is (= (frequencies [["Alice" "Acme Inc" "Bob" "Globex"]
                           ["Bob" "Globex" nil nil]])
             (column-values (tu/run-query q)
                            [label-var
                             (str "Company__via__worksFor__" label-var)
                             (str "P__" label-var)
                             (str "P__worksFor__" label-var)]))))))

(deftest ^:integration saved-question-as-source-test
  (let [q     (tu/native-card-query 9)
        label (tu/column q lib/fieldable-columns tu/rdfs-label)
        age   (tu/column q lib/filterable-columns "age")
        rows  #(:rows (tu/run-query (lib/with-fields % [label])))]
    (testing "a filter over a saved question"
      (is (= [["Alice"]] (rows (lib/filter q (lib/> age 26))))))
    (testing "the oldest person of a saved question"
      (is (= [["Alice"]] (rows (-> q (lib/order-by age :desc) (lib/limit 1)))))))
  (testing "a saved question joined to Company"
    ;; the joined subject is ?C__subject here, ?C_subject over a table
    (is (= company-join-rows
           (column-values (tu/run-query (left-join-company (tu/native-card-query 9)))
                          [label-var "C__subject" (str "C__" label-var) (str "C__headquarters__" label-var)])))))

(deftest ^:integration aggregation-over-aggregation-test
  (testing "the smallest and largest count of companies by headquarters (Springfield 2, Shelbyville 1)"
    (let [q      (tu/company-query)
          counts (-> q
                     (lib/breakout (tu/column q lib/breakoutable-columns "headquarters"))
                     (lib/aggregate (lib/count))
                     (lib/append-stage))
          n      (tu/column counts lib/filterable-columns "count")]
      (is (= [[1 2]] (:rows (tu/run-query (-> counts
                                              (lib/aggregate (lib/min n))
                                              (lib/aggregate (lib/max n))))))))))

(deftest ^:integration native-text-parameter-test
  (testing "a {{name}} text tag set to Bob keeps only Bob"
    (let [q (lib/query tu/provider
                       {:database   1
                        :type       :native
                        :native     {:query         (str "PREFIX ex: <" tu/ex ">\n"
                                                         "SELECT ?name WHERE { ?s a ex:Person ; <" tu/rdfs-label "> ?name "
                                                         "FILTER(?name = {{name}}) }")
                                     :template-tags {"name" {:name "name" :display-name "Name" :type :text
                                                             :id "00000000-0000-4000-8000-000000000001"}}}
                        :parameters [{:type :category :target [:variable [:template-tag "name"]] :value "Bob"}]})]
      (is (= [["Bob"]] (:rows (tu/run-query q)))))))

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
    ;; Premise: the endpoint answers a SPARQL parse error with HTTP 400, which
    ;; classifies as :query → invalid-query (Oxigraph, Fuseki, RDF4J and
    ;; Virtuoso do). An engine added to bin/lib.sh that answered 200 + an error
    ;; JSON would take the success branch instead, so check this test when
    ;; adding one.
    (let [e (is (thrown? clojure.lang.ExceptionInfo
                         (tu/run-native "SELECT ?x WHERE {")))]
      (is (= driver-api/qp.error-type.invalid-query (:type (ex-data e))))
      (is (str/includes? (ex-message e) "Error executing SPARQL query")))))
