(ns metabase.driver.sparql.test-util
  "Shared support for the :integration suites (smoke_test.clj, e2e_test.clj):
  the live-endpoint configuration and skip guard, plus a minimal Lib metadata
  provider mirroring the smoke fixture (test/resources/fixtures/smoke.ttl) and
  runners that drive the real driver multimethods under a QP store.

  The provider is hand-rolled because Metabase's mock providers live under
  metabase/test/, which is not on this project's test classpath."
  (:require [metabase.driver :as driver]
            [metabase.driver-api.core :as driver-api]
            [metabase.driver.sparql]
            [metabase.lib.core :as lib]
            [metabase.lib.metadata :as lib.metadata]
            [metabase.lib.metadata.protocols :as lib.metadata.protocols]))

(def endpoint
  "Live SPARQL endpoint under test (set by bin/smoke-test.sh)."
  (or (System/getenv "SPARQL_TEST_ENDPOINT") "http://localhost:7878/query"))

;; The fixture is loaded into this named graph; the driver also sends it as the
;; ?default-graph-uri protocol param, so sync/queries see the fixture triples.
;; bin/smoke-test.sh derives the seed target graph from SPARQL_TEST_GRAPH as well,
;; so the seeder and this constant share a single source of truth.
(def default-graph
  "Named graph the fixture is seeded into."
  (or (System/getenv "SPARQL_TEST_GRAPH") "https://example.org/"))

(def db
  "Database map in the shape the driver's sync/execute fns take directly."
  {:details {:endpoint endpoint :default-graph default-graph}})

(defn skip-without-live-endpoint
  "`use-fixtures :once` guard for every :integration namespace: only run against
  a live endpoint (i.e. under `make smoke`, which sets SPARQL_TEST_ENDPOINT and
  seeds Oxigraph). Under any tag-agnostic runner (cloverage / `make coverage`,
  or a bare `clojure -X:test`) the env is unset and the whole namespace is
  skipped, so no live HTTP is ever attempted."
  [t]
  (when (System/getenv "SPARQL_TEST_ENDPOINT")
    (t)))

(def rdfs-label
  "Name of the fixture's label column: a foreign-prefix property, so sync keeps
  it as its full URI."
  "http://www.w3.org/2000/01/rdf-schema#label")

(def ^:private person-table
  {:lib/type     :metadata/table
   :id           100
   :db-id        1
   :name         "Person"
   :display-name "Person"})

(defn- col [id position col-name base-type database-type]
  {:lib/type      :metadata/column
   :id            id
   :table-id      (:id person-table)
   :position      position
   :name          col-name
   :display-name  col-name
   :base-type     base-type
   :database-type database-type})

(def ^:private person-fields
  ;; Mirrors what describe-table discovers for Person in the smoke fixture
  ;; (provider-matches-live-schema-test in e2e_test.clj keeps the two in sync).
  ;; age is declared Integer so Lib accepts numeric filters on it; the compiler
  ;; itself only reads :name/:table-id. knows carries the "uri" marker the
  ;; auto-sync discovery now stamps on IRI-valued properties (?isIri), so
  ;; equality filters on it compile to <iri> terms.
  [(col 101 0 "subject" :type/Text "uri")
   (col 102 1 rdfs-label :type/Text "string")
   (col 103 2 "age" :type/Integer "string")
   (col 104 3 "knows" :type/Text "uri")])

(def provider
  "Minimal MetadataProvider over the fixture's schema. Carries the endpoint and
  default graph in the database :details, which is where both mbql->native and
  execute-reducible-query read them from."
  (reify lib.metadata.protocols/MetadataProvider
    (database [_]
      {:lib/type :metadata/database
       :id       1
       :engine   :sparql
       :features #{}
       :details  (:details db)})
    (metadatas [_ metadata-spec]
      (let [objects (case (:lib/type metadata-spec)
                      :metadata/table  [person-table]
                      :metadata/column person-fields
                      [])]
        (into []
              (lib.metadata.protocols/default-spec-filter-xform metadata-spec)
              objects)))
    (setting [_ _] nil)))

(defn person-query
  "A fresh pMBQL query over the Person table."
  []
  (lib/query provider (lib.metadata/table provider (:id person-table))))

(defn column
  "The column named col-name among (columns-fn q); columns-fn defaults to
  lib/fieldable-columns. Throws when absent, so fixture/schema drift fails at
  the lookup instead of as an opaque error deep inside Lib."
  ([q col-name]
   (column q lib/fieldable-columns col-name))
  ([q columns-fn col-name]
   (or (first (filter #(= (:name %) col-name) (columns-fn q)))
       (throw (ex-info (str "No column named " col-name)
                       {:available (map :name (columns-fn q))})))))

(defn- respond->map [metadata rows]
  {:cols (vec (:cols metadata))
   :rows (mapv vec rows)})

(defn- execute! [native]
  (driver-api/with-metadata-provider provider
    (driver/execute-reducible-query :sparql {:native native} nil respond->map)))

(defn run-native
  "Execute a raw SPARQL string through driver/execute-reducible-query under the
  test provider. Returns {:cols [...] :rows [...]}."
  [sparql]
  (execute! {:query sparql}))

(defn run-query
  "Compile a pMBQL query with driver/mbql->native and execute it end to end.
  Returns {:cols [...] :rows [...] :native \"SELECT ...\"}."
  [pmbql-query]
  (let [native (driver-api/with-metadata-provider provider
                 (driver/mbql->native :sparql pmbql-query))]
    (assoc (execute! native) :native (:query native))))
