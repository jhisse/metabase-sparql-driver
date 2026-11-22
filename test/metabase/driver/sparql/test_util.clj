(ns metabase.driver.sparql.test-util
  "Shared test support: the live-endpoint configuration and skip guard for the
  :integration suites (smoke_test.clj, e2e_test.clj), a minimal Lib metadata
  provider mirroring the smoke fixture (test/resources/fixtures/smoke.ttl),
  runners that drive the real driver multimethods under a QP store, and a
  SPARQL syntax oracle for generated queries.

  The provider is hand-rolled because Metabase's mock providers live under
  metabase/test/, which is not on this project's test classpath."
  (:require [metabase.driver :as driver]
            [metabase.driver-api.core :as driver-api]
            [metabase.driver.sparql]
            [metabase.lib.core :as lib]
            [metabase.lib.metadata :as lib.metadata]
            [metabase.lib.metadata.protocols :as lib.metadata.protocols]
            [metabase.query-processor.compile :as qp.compile])
  (:import [org.eclipse.rdf4j.query MalformedQueryException]
           [org.eclipse.rdf4j.query.parser.sparql SPARQLParser]))

(defn sparql-syntax-error
  "Parse `query` with RDF4J's SPARQL 1.1 parser. Returns nil when it parses,
  otherwise the parser's error message. Use as `(is (nil? (sparql-syntax-error q)) q)`:
  asserting on fragments with `str/includes?` passes even when the query around
  them is malformed."
  [query]
  (try
    (SPARQLParser/.parseQuery (SPARQLParser.) query nil)
    nil
    (catch MalformedQueryException e
      (ex-message e))))

(def endpoint
  "Live SPARQL endpoint under test (set by bin/smoke-test.sh; Oxigraph's by default)."
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

(def shacl-db
  "[[db]] synced with the `shacl` strategy from the fixture's shapes
  (test/resources/fixtures/smoke-shapes.ttl), which bin/smoke-test.sh seeds into
  their own named graph and exports as SPARQL_TEST_SHACL_URL (a Graph Store GET,
  `read_url` in bin/lib.sh; Oxigraph's by default)."
  (update db :details assoc
          :metadata-sync-strategy "shacl"
          :shacl-url (or (System/getenv "SPARQL_TEST_SHACL_URL")
                         "http://localhost:7878/store?graph=https%3A%2F%2Fexample.org%2Fshapes")))

(defn skip-without-live-endpoint
  "`use-fixtures :once` guard for every :integration namespace: only run against
  a live endpoint (i.e. under `make smoke`, which sets SPARQL_TEST_ENDPOINT and
  seeds the endpoint). Under any tag-agnostic runner (cloverage / `make coverage`,
  or a bare `clojure -X:test`) the env is unset and the whole namespace is
  skipped, so no live HTTP is ever attempted."
  [t]
  (when (System/getenv "SPARQL_TEST_ENDPOINT")
    (t)))

(defn skip-without-metabase
  "`use-fixtures :once` guard for metabase_api_test.clj: only run against the
  Metabase that `make e2e` starts (bin/demo.sh) and exports as METABASE_E2E_URL.
  Skipped under `make smoke`, `make coverage` and bare runners, like
  [[skip-without-live-endpoint]]."
  [t]
  (when (System/getenv "METABASE_E2E_URL")
    (t)))

(def ex
  "Namespace of the fixture's classes, properties and resources (smoke.ttl's `ex:`)."
  "https://example.org/")

(def rdfs-label
  "Name of the fixture's label column: a foreign-prefix property, so sync keeps
  it as its full URI."
  "http://www.w3.org/2000/01/rdf-schema#label")

(defn- table [id table-name]
  {:lib/type     :metadata/table
   :id           id
   :db-id        1
   :name         table-name
   :display-name table-name})

(def ^:private person-table (table 100 "Person"))
(def ^:private company-table (table 200 "Company"))
(def ^:private city-table (table 300 "City"))

(defn- col [table id position col-name base-type database-type]
  {:lib/type      :metadata/column
   :id            id
   :table-id      (:id table)
   :position      position
   :name          col-name
   :display-name  col-name
   :base-type     base-type
   :database-type database-type})

(defn- fk
  "Make `column` a FK to the subject field `target-id`, displayed as the field
  `label-id` through an external remap: what SHACL sync and the post-sync hook
  (dimensions.clj) record for a `sh:class` property with
  `sban:displayValueProperty`. The remap has the keys of Metabase's own test
  providers (metabase.lib.test-util.metadata-providers.remap), with the `:name`
  dimensions.clj records: the FK's display name."
  [column target-id label-id]
  (assoc column
         :semantic-type      :type/FK
         :fk-target-field-id target-id
         :lib/external-remap {:lib/type :metadata.column.remapping/external
                              :id       (* 10 (:id column))
                              :name     (:display-name column)
                              :field-id label-id}))

(def ^:private fields
  ;; Mirrors what describe-table discovers in the smoke fixture
  ;; (provider-matches-live-schema-test in e2e_test.clj checks that every
  ;; column name exists there), typed after smoke-shapes.ttl so Lib accepts numeric, temporal and boolean
  ;; filters (auto-sync types every literal as text). The compiler itself only
  ;; reads :name/:table-id. IRI-valued properties carry the "uri" marker
  ;; sync stamps on them, so equality filters compile to <iri> terms. The FKs
  ;; and their display values mirror smoke-shapes.ttl (sh:class and
  ;; sban:displayValueProperty rdfs:label); shacl-fks-test in smoke_test.clj
  ;; checks the three FKs and their target tables on the live SHACL sync, and
  ;; metabase_api_test.clj the display-value remap in a real Metabase.
  [(col person-table 101 0 "subject" :type/Text "uri")
   (col person-table 102 1 rdfs-label :type/Text "string")
   (col person-table 103 2 "age" :type/Integer "string")
   (fk (col person-table 104 3 "knows" :type/Text "uri") 101 102)
   (col person-table 105 4 "birthDate" :type/Date "string")
   (col person-table 106 5 "updated" :type/DateTime "string")
   ;; Language-tagged in the fixture; "langString" is what SHACL sync stamps on
   ;; rdf:langString properties, which the Default Language filter reads.
   (col person-table 107 6 "nickname" :type/Text "langString")
   (fk (col person-table 108 7 "worksFor" :type/Text "uri") 201 202)
   (col company-table 201 0 "subject" :type/Text "uri")
   (col company-table 202 1 rdfs-label :type/Text "string")
   (col company-table 203 2 "foundedOn" :type/Date "string")
   (col company-table 204 3 "employees" :type/Integer "string")
   (col company-table 205 4 "revenue" :type/Float "string")
   (col company-table 206 5 "listed" :type/Boolean "string")
   (fk (col company-table 207 6 "headquarters" :type/Text "uri") 301 302)
   (col city-table 301 0 "subject" :type/Text "uri")
   (col city-table 302 1 rdfs-label :type/Text "string")
   (col city-table 303 2 "population" :type/Integer "string")])

(def native-card
  "A saved native question over the fixture: a question built on it compiles
  the SPARQL below as the source of its MBQL stage."
  {:lib/type        :metadata/card
   :id              1
   :database-id     1
   :name            "People by age"
   :type            :question
   :dataset-query   {:database 1
                     :type     :native
                     :native   {:query (str "PREFIX ex: <https://example.org/>\n"
                                            "SELECT ?name ?age WHERE { ?s a ex:Person ; "
                                            "<" rdfs-label "> ?name ; ex:age ?age }")}}
   :result-metadata [{:lib/type :metadata/column :name "name" :display-name "name"
                      :base-type :type/Text :effective-type :type/Text}
                     {:lib/type :metadata/column :name "age" :display-name "age"
                      :base-type :type/Integer :effective-type :type/Integer}]})

(def native-cards
  "[[native-card]], plus cards over other SPARQL: an ASK query (id 2); a
  prologue with a comment and BASE ahead of a SELECT that ends in a comment
  (id 3); FROM and FROM NAMED clauses, with a full and a prefixed IRI (id 4); a non-ASCII variable (id 5);
  a Virtuoso DEFINE pragma (id 6); SELECT * with no recorded columns (id 7); a saved MBQL
  question on card 5, so a question on it nests two stages (id 8); a saved MBQL question
  over the Person table (id 9)."
  (let [with-query (fn [id sparql & [cols]]
                     (cond-> (-> native-card
                                 (assoc :id id)
                                 (assoc-in [:dataset-query :native :query] sparql))
                       cols (assoc :result-metadata cols)))
        person     (str "?s a ex:Person ; <" rdfs-label "> ?name ; ex:age ?age")
        idade-cols [{:lib/type :metadata/column :name "name" :display-name "name"
                     :base-type :type/Text :effective-type :type/Text}
                    {:lib/type :metadata/column :name "idade_média" :display-name "idade_média"
                     :base-type :type/Integer :effective-type :type/Integer}]]
    [native-card
     (with-query 2 "ASK { ?s a <https://example.org/Person> }")
     (with-query 3 (str "# people and their ages\n"
                        "BASE <https://example.org/>\n"
                        "PREFIX ex: <https://example.org/>\n"
                        "SELECT ?name ?age WHERE { ?s a ex:Person ; "
                        "<" rdfs-label "> ?name ; ex:age ?age } # no LIMIT"))
     (with-query 4 (str "PREFIX ex: <https://example.org/>\n"
                        "SELECT ?name ?age FROM <https://example.org/g> FROM NAMED ex:h "
                        "WHERE { " person " }"))
     (with-query 5 (str "PREFIX ex: <https://example.org/>\n"
                        "SELECT ?name (?age AS ?idade_média) WHERE { " person " }")
       idade-cols)
     (with-query 6 (str "DEFINE input:inference \"r\"\n"
                        "PREFIX ex: <https://example.org/>\n"
                        "SELECT ?name ?age WHERE { " person " }"))
     (with-query 7 (str "PREFIX ex: <https://example.org/>\n"
                        "SELECT * WHERE { " person " }")
       [])
     (-> (with-query 8 "" idade-cols)
         (assoc :dataset-query {:database 1 :type :query :query {:source-table "card__5"}}))
     (-> (with-query 9 "")
         (dissoc :result-metadata)
         (assoc :dataset-query {:database 1 :type :query :query {:source-table (:id person-table)}}))]))

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
                      :metadata/table  [person-table company-table city-table]
                      :metadata/column fields
                      :metadata/card   native-cards
                      [])]
        (into []
              (lib.metadata.protocols/default-spec-filter-xform metadata-spec)
              objects)))
    (setting [_ _] nil)))

(defn- table-query [table]
  (lib/query provider (lib.metadata/table provider (:id table))))

(defn person-query
  "A fresh pMBQL query over the Person table."
  []
  (table-query person-table))

(defn company-query
  "A fresh pMBQL query over the Company table."
  []
  (table-query company-table))

(defn city-query
  "A fresh pMBQL query over the City table."
  []
  (table-query city-table))

(defn native-card-query
  "A fresh pMBQL query whose source is the card `id` of [[native-cards]],
  [[native-card]] by default."
  ([] (native-card-query (:id native-card)))
  ([id] (lib/query provider (lib.metadata/card provider id))))

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

(defn compile-query
  "Compile a pMBQL query the way the QP does — preprocessing middleware
  (desugaring, date-literal and value wrapping, …) then driver/mbql->native —
  without an app DB. Calling driver/mbql->native directly would skip the
  preprocessing, so it would compile queries the driver never receives.
  Returns the native map {:query \"SELECT ...\" …}."
  [pmbql-query]
  (driver-api/with-metadata-provider provider
    (qp.compile/compile pmbql-query)))

(defn run-query
  "Compile a pMBQL query with [[compile-query]] and execute it end to end.
  Returns {:cols [...] :rows [...]}."
  [pmbql-query]
  (execute! (compile-query pmbql-query)))
