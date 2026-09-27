(ns metabase.driver.sparql.metabase-api-test
  "Tests through a real Metabase: the built driver jar loaded as a plugin,
  synced by Metabase's own sync, and queried through its REST API. Covers what
  smoke_test.clj and e2e_test.clj cannot, since they call the driver in-process:
  plugin loading, what sync persists (FK targets, the SHACL display-value
  remap written by dimensions.clj), and legacy MBQL as the frontend sends it
  through the full QP middleware stack.

  Driven by `make e2e`, which starts the environment with bin/demo.sh and
  points METABASE_E2E_URL at it. The environment details (login, database ids)
  come from bin/demo.sh's target/demo/env.json."
  (:require [clj-http.client :as http]
            [clojure.test :refer :all]
            [metabase.driver.sparql.test-util :as tu]
            [metabase.util.json :as json]))

(use-fixtures :once tu/skip-without-metabase)

(def ^:private env
  (delay (json/decode+kw (slurp "target/demo/env.json"))))

(def ^:private session
  (delay (-> (http/post (str (System/getenv "METABASE_E2E_URL") "/api/session")
                        {:content-type  :json
                         :cookie-policy :standard
                         :body         (json/encode {:username (:email @env) :password (:password @env)})})
             :body
             json/decode+kw
             :id)))

(defn- api [method path & [body]]
  (-> (http/request (cond-> {:method        method
                             :url           (str (System/getenv "METABASE_E2E_URL") path)
                             :cookie-policy :standard
                             :headers       {"X-Metabase-Session" @session}}
                      body (assoc :content-type :json :body (json/encode body))))
      :body
      json/decode+kw))

(defn- db-id [k] (get-in @env [:databases k]))

(defn- tables
  "Synced tables of database `k` (:auto or :shacl), by name, each with its
  fields by name."
  [k]
  (into {}
        (map (fn [t] [(:name t) (assoc t :fields (into {} (map (juxt :name identity)) (:fields t)))]))
        (:tables (api :get (format "/api/database/%d/metadata" (db-id k))))))

(defn- field-ref [table field-name]
  [:field (get-in table [:fields field-name :id]) nil])

(defn- run-mbql
  "Run a legacy MBQL `query` against database `k` through /api/dataset, the
  endpoint the query builder uses. Returns {:cols [names…] :rows […]}."
  [k query]
  (let [{:keys [status error data]} (api :post "/api/dataset" {:database (db-id k) :type :query :query query})]
    (is (= "completed" status) error)
    {:cols (mapv :name (:cols data)) :rows (:rows data)}))

(deftest ^:integration plugin-syncs-auto-strategy-test
  (testing "the plugin loads and Metabase's sync discovers the fixture classes"
    (is (= #{"Person" "Company" "City"} (set (keys (tables :auto)))))))

(deftest ^:integration shacl-sync-persists-types-and-fks-test
  (let [{:strs [Person Company City]} (tables :shacl)]
    (is (= "A company that employs people" (:description Company)))
    (is (= "type/Integer" (get-in Person [:fields "age" :base_type])))
    (is (= "type/Date" (get-in Person [:fields "birthDate" :base_type])))
    (testing "sh:class shapes are saved as FKs to the target table's subject"
      (is (= (get-in City [:fields "subject" :id])
             (get-in Company [:fields "headquarters" :fk_target_field_id]))))))

(deftest ^:integration filter-through-api-test
  (let [person (get (tables :shacl) "Person")]
    (is (= [["Alice"]]
           (:rows (run-mbql :shacl {:source-table (:id person)
                                    :fields       [(field-ref person tu/rdfs-label)]
                                    :filter       [:> (field-ref person "age") 26]}))))))

(deftest ^:integration aggregation-through-api-test
  (let [company (get (tables :shacl) "Company")]
    (testing "count"
      (is (= [[3]] (:rows (run-mbql :shacl {:source-table (:id company) :aggregation [[:count]]})))))
    (testing "sum grouped by a boolean"
      (is (= #{[false 45] [true 420]}
             (set (:rows (run-mbql :shacl {:source-table (:id company)
                                           :aggregation  [[:sum (field-ref company "employees")]]
                                           :breakout     [(field-ref company "listed")]}))))))))

(deftest ^:integration fk-display-value-remap-test
  (testing "the Dimension written after sync makes Metabase add the target's label"
    (let [person (get (tables :shacl) "Person")
          {:keys [cols rows]} (run-mbql :shacl {:source-table (:id person)
                                                :fields       [(field-ref person tu/rdfs-label)
                                                               (field-ref person "worksFor")]})]
      (is (= 3 (count cols)) "label, worksFor, and the remapped label")
      (is (= #{["Alice" "https://example.org/acme" "Acme Inc"]
               ["Bob" "https://example.org/globex" "Globex"]}
             (set rows))))))

(deftest ^:integration missing-fk-display-value-test
  ;; Bob has no `knows`: the remapped label must stay empty for him instead of
  ;; matching every rdfs:label in the graph.
  (let [person (get (tables :shacl) "Person")
        knows  (get-in person [:fields "knows" :id])
        label  (get-in person [:fields tu/rdfs-label :id])]
    (testing "the default table view keeps one row per Person"
      (is (= 2 (count (:rows (run-mbql :shacl {:source-table (:id person)}))))))
    (testing "filtering the default view by the label a self-referencing FK also displays"
      (is (= 1 (count (:rows (run-mbql :shacl {:source-table (:id person)
                                               :filter       [:= [:field label nil] "Alice"]}))))))
    (testing "grouping by the label behind a missing FK"
      (is (= #{[nil 25] ["Bob" 30]}
             (set (:rows (run-mbql :shacl {:source-table (:id person)
                                           :aggregation  [[:sum (field-ref person "age")]]
                                           :breakout     [[:field label {:source-field knows}]]}))))))))

(deftest ^:integration filter-on-aggregation-over-fk-breakout-test
  (testing "an outer filter on the count resolves to the count, not to the FK column"
    (let [company (get (tables :shacl) "Company")]
      (is (= [["https://example.org/springfield" 2 "Springfield"]]
             (:rows (run-mbql :shacl {:source-query {:source-table (:id company)
                                                     :aggregation  [[:count]]
                                                     :breakout     [(field-ref company "headquarters")]}
                                      :filter       [:> [:field "count" {:base-type :type/Integer}] 1]})))))))

(deftest ^:integration breakout-without-aggregation-test
  (let [company (get (tables :shacl) "Company")]
    (testing "a breakout alone returns distinct values (two companies share Springfield)"
      (is (= 2 (count (:rows (run-mbql :shacl {:source-table (:id company)
                                               :breakout     [(field-ref company "headquarters")]}))))))
    (testing "a field's filter-value list has no duplicates"
      (is (= [[false] [true]]
             (:values (api :get (format "/api/field/%d/values"
                                        (get-in company [:fields "listed" :id])))))))))

(deftest ^:integration explicit-join-with-remapped-fk-test
  (testing "a FK display value inside an explicit join reads the FK's target"
    (let [{:strs [Person Company]} (tables :shacl)
          {:keys [cols rows]}
          (run-mbql :shacl {:source-table (:id Person)
                            :fields       [(field-ref Person tu/rdfs-label)]
                            :joins        [{:source-table (:id Company)
                                            :alias        "C"
                                            :condition    [:= (field-ref Person "worksFor")
                                                           [:field (get-in Company [:fields "subject" :id]) {:join-alias "C"}]]
                                            :fields       [[:field (get-in Company [:fields tu/rdfs-label :id]) {:join-alias "C"}]
                                                           [:field (get-in Company [:fields "headquarters" :id]) {:join-alias "C"}]]}]})]
      (is (= 4 (count cols)) "label, C's label and headquarters, and the headquarters' label")
      (is (= #{["Alice" "Acme Inc" "https://example.org/springfield" "Springfield"]
               ["Bob" "Globex" "https://example.org/springfield" "Springfield"]}
             (set rows))))))

(deftest ^:integration result-column-display-names-test
  (testing "MBQL result columns keep Metabase's display names, not SPARQL var names"
    (let [company (get (tables :shacl) "Company")
          {:keys [data]} (api :post "/api/dataset"
                              {:database (db-id :shacl)
                               :type     :query
                               :query    {:source-table (:id company)
                                          :aggregation  [[:sum (field-ref company "revenue")]]}})]
      (is (= ["Sum of Revenue"] (mapv :display_name (:cols data)))))))

(deftest ^:integration foreign-uri-field-display-name-test
  (testing "a property outside the Default Graph (rdfs:label) reads as its local name"
    (is (= "Label" (get-in (tables :shacl) ["Person" :fields tu/rdfs-label :display_name])))))

(deftest ^:integration native-query-through-api-test
  (let [{:keys [status error data]}
        (api :post "/api/dataset"
             {:database (db-id :auto)
              :type     :native
              :native   {:query "SELECT ?label WHERE { ?c a <https://example.org/City> ;
                                   <http://www.w3.org/2000/01/rdf-schema#label> ?label } ORDER BY ?label"}})]
    (is (= "completed" status) error)
    (is (= [["Shelbyville"] ["Springfield"]] (:rows data)))))
