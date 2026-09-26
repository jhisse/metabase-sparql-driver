(ns metabase.driver.sparql.dimensions-test
  "Unit tests for the SHACL displayValueProperty -> Dimension post-sync hook.
   The SHACL fetch and every app-DB access (field lookup, toucan2 reads and
   writes) are stubbed, so no Metabase application database is needed."
  (:require [clojure.test :refer :all]
            [metabase.driver.sparql.dimensions :as dimensions]
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
    (with-redefs-fn {#'dimensions/shacl-shapes      (constantly shapes)
                     #'dimensions/field-for         (fn [db-id table field]
                                                      (is (= 1 db-id))
                                                      (synced-fields [table field]))
                     #'dimensions/upsert-dimension! (fn [& args] (swap! upserts conj (vec args)))}
      (fn []
        (dimensions/sync-display-dimensions! {:id 1 :details {:default-graph graph}})
        (testing "only FK properties with a display property whose both ends are synced are upserted,
                  with URIs shortened to the synced table/field names"
          (is (= [[10 "geboorteplaats" 20]] @upserts)))))))

(deftest sync-display-dimensions-survives-upsert-failure-test
  (testing "a failing upsert is logged and does not abort the sync hook"
    (with-redefs-fn {#'dimensions/shacl-shapes      (constantly shapes)
                     #'dimensions/field-for         (fn [_ table field] (synced-fields [table field]))
                     #'dimensions/upsert-dimension! (fn [& _] (throw (ex-info "db down" {})))}
      (fn []
        (is (nil? (dimensions/sync-display-dimensions! {:id 1 :details {:default-graph graph}})))))))

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
