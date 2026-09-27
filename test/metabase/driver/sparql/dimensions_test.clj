(ns metabase.driver.sparql.dimensions-test
  "Unit tests for the SHACL displayValueProperty -> Dimension post-sync hook.
   The SHACL fetch and every app-DB access (field lookup, toucan2 reads and
   writes) are stubbed, so no Metabase application database is needed."
  (:require [clojure.test :refer :all]
            [metabase.driver.sparql.database :as database]
            [metabase.driver.sparql.dimensions :as dimensions]
            [metabase.driver.sparql.shacl :as shacl]
            [metabase.models.humanization :as humanization]
            [toucan2.core :as t2]))

(def ^:private graph "https://example.org/")

(def ^:private shapes
  [{:class-uri  (str graph "Persoon")
    :properties [{:property-uri           (str graph "geboorteplaats")
                  :fk-target-class        (str graph "Plaats")
                  :display-value-property (str graph "label")}
                 ;; FK without a display property: no dimension
                 {:property-uri    (str graph "werkgever")
                  :fk-target-class (str graph "Bedrijf")}
                 ;; display property without an FK target: no dimension
                 {:property-uri           (str graph "naam")
                  :display-value-property (str graph "label")}
                 ;; target table not synced yet: skipped
                 {:property-uri           (str graph "school")
                  :fk-target-class        (str graph "School")
                  :display-value-property (str graph "label")}]}])

(def ^:private synced-fields
  {["Persoon" "geboorteplaats"] {:id 10}
   ["Persoon" "school"]         {:id 11}
   ["Plaats" "label"]           {:id 20}})

(deftest sync-display-dimensions-test
  (let [upserts (atom [])]
    (with-redefs-fn {#'database/shacl-shapes        (constantly shapes)
                     #'dimensions/field-for         (fn [db-id table field]
                                                      (is (= 1 db-id))
                                                      (synced-fields [table field]))
                     #'dimensions/upsert-dimension! (fn [& args] (swap! upserts conj (vec args)))}
      (fn []
        (dimensions/sync-display-dimensions! {:id 1 :details {:default-graph graph}})
        (testing "only FK properties with a display property whose both ends are synced are upserted,
                  with URIs shortened to the synced table/field names"
          (is (= [[10 "geboorteplaats" 20]] @upserts)))))))

(deftest sync-display-dimensions-outside-shacl-strategy-test
  (testing "with a SHACL URL but another sync strategy, the hook neither fetches nor writes"
    (let [fetched? (atom false)
          upserts  (atom [])]
      (with-redefs-fn {#'shacl/metadata               (fn [& _] (reset! fetched? true) shapes)
                       #'dimensions/field-for         (fn [_ table field] (synced-fields [table field]))
                       #'dimensions/upsert-dimension! (fn [& args] (swap! upserts conj (vec args)))}
        #(dimensions/sync-display-dimensions!
          {:id 1 :details {:default-graph          graph
                           :shacl-url              "https://example.org/shapes.ttl"
                           :metadata-sync-strategy "auto"}}))
      (is (false? @fetched?))
      (is (= [] @upserts)))))

(deftest sync-display-dimensions-survives-upsert-failure-test
  (testing "a failing upsert is logged and does not abort the sync hook"
    (with-redefs-fn {#'database/shacl-shapes        (constantly shapes)
                     #'dimensions/field-for         (fn [_ table field] (synced-fields [table field]))
                     #'dimensions/upsert-dimension! (fn [& _] (throw (ex-info "db down" {})))}
      (fn []
        (is (nil? (dimensions/sync-display-dimensions! {:id 1 :details {:default-graph graph}})))))))

(deftest field-for-skips-retired-tables-test
  (testing "the lookup only matches fields of active tables"
    (let [query (atom nil)]
      (with-redefs [t2/select-one (fn [_ q] (reset! query q) nil)]
        (#'dimensions/field-for 1 "Persoon" "naam"))
      (is (some #{[:= :t.active true]} (:where @query))))))

(deftest sync-display-names-skips-retired-tables-test
  (testing "display names are only fixed on fields of active tables"
    (let [query (atom nil)]
      (with-redefs [t2/select (fn [_ q] (reset! query q) [])]
        (#'dimensions/sync-display-names! {:id 1}))
      (is (some #{[:= :t.active true]} (:where @query))))))

(deftest sync-end-runs-each-step-test
  (testing "a failing Dimension sync does not skip the display-name fix"
    (let [named (atom nil)]
      (with-redefs [t2/select-one                          (constantly {:id 1 :engine "sparql"})
                    dimensions/sync-display-dimensions!    (fn [_] (throw (ex-info "db down" {})))
                    dimensions/sync-display-names!         (fn [db] (reset! named (:id db)))]
        (#'dimensions/sync-end! 1))
      (is (= 1 @named)))))

(deftest upsert-dimension-test
  (letfn [(writes-for [existing display-field-id]
            (let [writes (atom [])
                  log    (fn [op] (fn [& args] (swap! writes conj (into [op] (rest args)))))]
              (with-redefs [t2/select-one (constantly existing)
                            t2/insert!    (log :insert)
                            t2/update!    (log :update)]
                (#'dimensions/upsert-dimension! 10 "geboorteplaats" display-field-id))
              @writes))]
    (let [existing {:id 5 :type :external :name "geboorteplaats" :human_readable_field_id 20}]
      (testing "no existing row: insert an external remap"
        (is (= [[:insert {:field_id 10 :type :external :name "geboorteplaats" :human_readable_field_id 20}]]
               (writes-for nil 20))))
      (testing "an identical row is left alone (idempotent across syncs)"
        (is (= [] (writes-for existing 20))))
      (testing "a changed display field updates the existing row in place"
        (is (= [[:update 5 {:type :external :name "geboorteplaats" :human_readable_field_id 21}]]
               (writes-for existing 21)))))))

(deftest readable-display-name-test
  (let [readable (fn [field-name display-name]
                   (#'dimensions/readable-display-name {:name field-name :display_name display-name}))
        label    "http://www.w3.org/2000/01/rdf-schema#label"]
    (testing "a full-URI field with sync's default display name gets its local name"
      (is (= "Label" (readable label "Http://www.w3.org/2000/01/rdf Schema#label"))))
    (testing "a display name an admin changed is kept"
      (is (nil? (readable label "Name"))))
    (testing "no write when the local name is the whole URI"
      (let [urn "urn:isbn:123"]
        (is (nil? (readable urn (humanization/name->human-readable-name urn))))))
    (testing "a short field name is left to sync"
      (is (nil? (readable "foundedOn" "Founded On"))))))
