(ns metabase.driver.sparql.database
  "Metadata sync for SPARQL endpoints: describe the tables (RDF classes), their
   fields (properties) and SHACL foreign keys, using the connection's sync
   strategy (auto, explicit, SHACL or none)."
  (:require [metabase.util.log :as log]
            [clojure.string :as str]
            [metabase.util.json :as json]
            [metabase.driver.sparql.auth :as auth]
            [metabase.driver.sparql.execute :as execute]
            [metabase.driver.sparql.shacl :as shacl]
            [metabase.driver.sparql.templates :as templates]
            [metabase.driver.sparql.uri :as uri]))

;; This coercion only exists because Metabase's manifest spec
;; (`build-drivers.lint-manifest-file/property-types`) rejects `integer`/`select`.
;; If a future Metabase core accepts those types again, the limit/timeout fields
;; can go back to `type: integer` and this helper can be removed.
(defn- ->long
  "Return the connection-property value `v` as a Long, or nil when it is nil,
   blank or not an integer, so callers can fall back to defaults. Manifest
   props are `type: string`; legacy configs may hold a number."
  [v]
  (when (some? v)
    (parse-long (str/trim (str v)))))

(defn- sync-strategy
  "Return the metadata sync strategy of connection `details` as a keyword,
   `:auto` when unset."
  [details]
  (keyword (get details :metadata-sync-strategy "auto")))

(defn- parse-schema-config
  "Parse the schema configuration JSON into `{:tables [...]}`, or return nil
   when it is blank or not valid JSON (the parse error is logged)."
  [config-str]
  (when-not (str/blank? config-str)
    (try
      (json/decode+kw config-str)
      (catch Exception e
        (log/errorf "Error parsing schema configuration: %s" (.getMessage e))
        nil))))

(defn- build-pk-field
  "Build the synthetic primary-key field for the RDF subject of each
   instance. Named `subject` to mirror the `?subject` variable used in the emitted
   SPARQL and to avoid collisions with shortened property URIs whose local name is
   `id` (a very common case once Default Graph stripping is in effect)."
  []
  {:name "subject"
   :database-type "uri"
   :base-type :type/Text
   :pk? true
   :database-position 0})

(defn- build-field-from-uri
  "Build the field for the property `field-uri` at position `idx` + 1
   (position 0 is the subject).

   `naming` (a [[uri/naming-context]]) shortens the URI when it matches the
   Default Graph or a configured namespace prefix, so the column name in
   Metabase is the short name (e.g. `naam`, `foaf__name`) instead of the
   full URI. The full URI is reconstructed at query-compile time.

   `iri?` (default false) marks a property whose values are IRI nodes: it
   syncs as `:database-type \"uri\"`, the same marker as the subject column,
   so equality filters compile to `<iri>` terms."
  ([naming idx field-uri]
   (build-field-from-uri naming idx field-uri false))
  ([naming idx field-uri iri?]
   {:name (uri/shorten-uri field-uri naming)
    :database-type (if iri? "uri" "string")
    :base-type :type/Text
    :pk? false
    :database-position (inc idx)}))

(defn- build-fields-from-explicit-config
  "Build the field set of an explicit-schema table: the `subject` key plus one
  field per configured property, without foreign URIs when `hide-foreign?`."
  [naming hide-foreign? explicit-table]
  (let [pk-field     (build-pk-field)
        candidates   (cond->> (:fields explicit-table)
                       hide-foreign? (remove #(uri/foreign-uri? % naming)))
        other-fields (map-indexed (partial build-field-from-uri naming) candidates)]
    (set (cons pk-field other-fields))))

(defn- iri-valued-binding?
  "True when a `class-properties-query` result row says every sampled value of
   the property is an IRI node (`?isIri` = 1). Tolerant of endpoints that
   return booleans."
  [binding]
  (contains? #{"1" "true"} (get-in binding [:isIri :value])))

(defn- build-fields-from-sparql-query
  "Build the field set of a discovered class from the property query's
  `bindings`: the `subject` key plus one field per property, without foreign
  URIs when `hide-foreign?`."
  [naming hide-foreign? bindings]
  (let [pk-field   (build-pk-field)
        candidates (cond->> bindings
                     hide-foreign? (remove #(uri/foreign-uri? (get-in % [:property :value]) naming)))
        other-fields (map-indexed
                      (fn [idx binding]
                        (build-field-from-uri naming idx
                                              (get-in binding [:property :value])
                                              (iri-valued-binding? binding)))
                      candidates)]
    (set (cons pk-field other-fields))))

(defn- describe-table-none
  "Return `table` with no fields: the none strategy skips sync."
  [table]
  (log/info "Skipping table metadata sync for SPARQL database - sync strategy is 'none'")
  {:name (:name table)
   :schema nil
   :fields #{}})

(defn- describe-table-explicit
  "Return the fields listed for `table` in the explicit schema configuration."
  [naming hide-foreign? table explicit-table]
  (log/info "Using explicit schema configuration for table:" (:name table))
  {:name (:name table)
   :schema nil
   :fields (build-fields-from-explicit-config naming hide-foreign? explicit-table)})

(defn- fetch-class-properties
  "Run the property-discovery query for `class-uri` and return the
   `[success result kind]` of [[execute/execute-sparql-query]], retrying
   without the `?isIri` projection when the endpoint *rejects the query
   itself* (kind `:query`).

   The projection is SPARQL 1.1 and works on the engines we test against, but
   an endpoint that refuses it would otherwise sync the table with zero fields,
   since the discovery query is all-or-nothing. The retry keeps such an
   endpoint working, minus the IRI marker. Endpoint/transport failures are not
   retried: a second round-trip would not fare better."
  [endpoint class-uri property-limit sample-limit options]
  (let [run (fn [detect-iri?]
              (execute/execute-sparql-query
               endpoint
               (templates/class-properties-query class-uri property-limit sample-limit detect-iri?)
               options))
        [success _ kind :as enriched] (run true)]
    (if (or success (not= :query kind))
      enriched
      (do (log/warnf (str "[sync] Endpoint rejected the IRI-detection query for %s; "
                          "retrying without it. IRI-valued properties will sync as plain "
                          "strings, so equality filters on them may not match.")
                     class-uri)
          (run false)))))

(defn- describe-table-auto
  "Discover the properties of `table` by sampling its instances, capped by the
  database's property limit (default 20) and sample limit (default 10000). A
  failed query logs and returns `{:fields #{}}`."
  [database table]
  (let [details        (:details database)
        naming         (uri/naming-context details)
        hide-foreign?  (boolean (:hide-foreign-uris details))
        endpoint       (:endpoint details)
        options        {:insecure?     (:use-insecure details)
                        :default-graph (:default-graph details)
                        :auth          (auth/http-options details)}
        class-uri      (uri/absolute-uri (:name table) naming)
        property-limit (or (->long (:property-limit details)) 20)
        sample-limit   (or (->long (:sample-limit details)) 10000)
        [success result] (fetch-class-properties endpoint class-uri property-limit sample-limit options)]
    (if success
      {:name (:name table)
       :schema nil
       :fields (build-fields-from-sparql-query naming hide-foreign? (get-in result [:results :bindings]))}
      (do
        (log/error "Error describing SPARQL table:" result)
        {:fields #{}}))))

;; ---- SHACL-driven sync ------------------------------------------------------

(defn- shacl-prop->field
  "Convert the SHACL property descriptor `prop` into a Metabase
   TableMetadataField at position `idx` + 1, or return nil for a foreign
   property when `hide-foreign?`."
  [naming hide-foreign? idx prop]
  (let [uri      (:property-uri prop)
        foreign? (uri/foreign-uri? uri naming)]
    (when-not (and foreign? hide-foreign?)
      (cond-> {:name              (uri/shorten-uri uri naming)
               :database-type     (cond
                                    (:lang-string? prop) "langString"
                                    ;; IRI-node values (sh:nodeKind sh:IRI or an
                                    ;; sh:class target): same marker as the
                                    ;; subject column, consumed by value->term.
                                    (:iri-kind? prop)    "uri"
                                    :else                "string")
               :base-type         (or (:base-type prop) :type/Text)
               :pk?               false
               :database-position (inc idx)}
        (:semantic-type prop)     (assoc :semantic-type (:semantic-type prop))
        (:description prop)       (assoc :field-comment (:description prop))
        (:database-required prop) (assoc :database-required true)))))

(defn- shacl-shape->table
  "Convert one SHACL shape into a Metabase TableMetadata `:table` entry."
  [naming {:keys [class-uri description]}]
  {:name         (uri/shorten-uri class-uri naming)
   :schema       nil
   :display-name (uri/local-name class-uri)
   :description  (or description (str "RDF Class: " class-uri " (SHACL)"))})

(defn- shacl-shape->describe-table
  "Convert one SHACL shape into the map returned by `driver/describe-table`.

   Field positions follow `sh:order` ascending, with `:property-uri` as a
   tie-breaker so they are deterministic; properties without `sh:order` come
   last."
  [naming hide-foreign? {:keys [class-uri properties]}]
  (let [pk-field   (build-pk-field)
        candidates (cond->> properties
                     hide-foreign? (remove #(uri/foreign-uri? (:property-uri %) naming))
                     :always       (sort-by (juxt #(or (:order %) Long/MAX_VALUE)
                                                  :property-uri)))
        fields     (->> candidates
                        (map-indexed (fn [idx p] (shacl-prop->field naming hide-foreign? idx p)))
                        (remove nil?))]
    {:name   (uri/shorten-uri class-uri naming)
     :schema nil
     :fields (set (cons pk-field fields))}))

(defn- shape-for-table
  "Return the SHACL shape whose class is the full URI of `table`, or nil."
  [shapes naming table]
  (let [full (uri/absolute-uri (:name table) naming)]
    (some #(when (= (:class-uri %) full) %) shapes)))

(defn- shacl-fetch-opts
  "Build the HTTP options map for the SHACL fetch from connection `details`.
   Timeouts are configured in seconds and the size cap in megabytes. Unset
   values, and values out of range (zero or negative, where a 0 timeout means
   no timeout to the HTTP client, or too large for the HTTP client), are left
   `nil` so the SHACL extractor applies its own defaults. Never throws, so a
   mistyped limit cannot turn into an empty schema."
  [details]
  (let [scaled (fn [v unit max-value]
                 (when-let [n (->long v)]
                   (when (<= 1 n (quot max-value unit))
                     (* n unit))))]
    {:connect-timeout-ms (scaled (:shacl-connect-timeout details) 1000 Integer/MAX_VALUE)
     :socket-timeout-ms  (scaled (:shacl-socket-timeout details) 1000 Integer/MAX_VALUE)
     :max-bytes          (scaled (:shacl-max-size-mb details) (* 1024 1024) Long/MAX_VALUE)}))

(defn shacl-shapes
  "Return the SHACL shapes of `database` (cached by [[shacl/metadata]]), or
   nil when the sync strategy is not `shacl`, no SHACL URL is configured, or
   the document cannot be fetched or parsed (the error is logged). The
   language for `sh:name`/`sh:description` and the HTTP timeouts and size cap
   come from the connection details."
  [database]
  (let [details (:details database)
        url     (not-empty (str/trim (str (:shacl-url details))))]
    (when (and url (= :shacl (sync-strategy details)))
      (try
        (shacl/metadata url
                        (or (:default-language details) "")
                        (shacl-fetch-opts details))
        (catch Exception t
          (log/errorf t "[shacl] Failed to load SHACL document at %s" url)
          nil)))))

(defn fks
  "Return the foreign keys declared by `sh:class` in the SHACL document of
   `database`, one `{:fk-table-name … :fk-column-name … :pk-table-name …
   :pk-column-name \"subject\"}` map (schemas nil) per property. Empty for
   non-SHACL sync strategies or when the shapes cannot be loaded. With
   `:hide-foreign-uris`, an FK is dropped when its class, property or target
   class is foreign."
  [database]
  (let [naming        (uri/naming-context (:details database))
        hide-foreign? (boolean (-> database :details :hide-foreign-uris))]
    (for [shape (shacl-shapes database)
          prop  (:properties shape)
          :let  [fk-class (:fk-target-class prop)
                 prop-uri (:property-uri prop)]
          :when fk-class
          :when (not (and hide-foreign?
                          (or (uri/foreign-uri? fk-class naming)
                              (uri/foreign-uri? prop-uri naming)
                              (uri/foreign-uri? (:class-uri shape) naming))))]
      {:fk-table-name   (uri/shorten-uri (:class-uri shape) naming)
       :fk-table-schema nil
       :fk-column-name  (uri/shorten-uri prop-uri naming)
       :pk-table-name   (uri/shorten-uri fk-class naming)
       :pk-table-schema nil
       :pk-column-name  "subject"})))

(defn- describe-database-shacl
  [database]
  (let [details       (:details database)
        naming        (uri/naming-context details)
        hide-foreign? (boolean (:hide-foreign-uris details))
        shapes        (shacl-shapes database)]
    (when-not shapes
      (log/warnf "[shacl] No shapes available for database %s; returning empty table set" (:name database)))
    {:tables (->> (or shapes [])
                  (remove (fn [s] (and hide-foreign?
                                       (uri/foreign-uri? (:class-uri s) naming))))
                  (map #(shacl-shape->table naming %))
                  set)}))

(defn- describe-table-shacl
  [database table]
  (let [details       (:details database)
        naming        (uri/naming-context details)
        hide-foreign? (boolean (:hide-foreign-uris details))
        shapes        (shacl-shapes database)
        match         (shape-for-table shapes naming table)]
    (if match
      (shacl-shape->describe-table naming hide-foreign? match)
      (do
        (log/warnf "[shacl] No shape found for table %s; returning empty fields"
                   (:name table))
        {:name (:name table) :schema nil :fields #{(build-pk-field)}}))))

(defn describe-table
  "Return the fields of `table`, an RDF class whose `:name` may be a shortened
   URI, as `{:name … :schema nil :fields #{…}}`.

   Uses the same sync strategy as [[describe-database]]; an explicit strategy
   that does not list the class falls back to auto."
  [_ database table]
  (let [details        (:details database)
        sync-strategy  (sync-strategy details)
        naming         (uri/naming-context details)
        hide-foreign?  (boolean (:hide-foreign-uris details))
        schema-config  (some-> details :schema-config parse-schema-config)
        full-name      (uri/absolute-uri (:name table) naming)
        explicit-table (when (= sync-strategy :explicit)
                         (some #(when (= (:name %) full-name) %) (:tables schema-config)))]
    (cond
      (= sync-strategy :none)
      (describe-table-none table)

      (= sync-strategy :shacl)
      (describe-table-shacl database table)

      (and (= sync-strategy :explicit) explicit-table)
      (describe-table-explicit naming hide-foreign? table explicit-table)

      :else
      (describe-table-auto database table))))

(defn- build-table-from-config
  "Build the table for a class listed in the explicit schema configuration."
  [naming table]
  (let [uri        (:name table)
        short-name (uri/shorten-uri uri naming)]
    {:name short-name
     :schema nil
     :display-name (uri/local-name uri)
     :description (or (:description table)
                      (str "RDF Class: " uri " (Explicit)"))}))

(defn- build-table-from-sparql-result
  "Build the table for a class found by the discovery query, with its instance
  count in the description."
  [naming {:keys [uri count]}]
  {:name (uri/shorten-uri uri naming)
   :schema nil
   :display-name (uri/local-name uri)
   :description (str "RDF Class: " uri " (Instances: " count ")")})

(defn- describe-database-none
  "Return no tables: the none strategy skips sync."
  []
  (log/info "Skipping metadata sync for SPARQL database - sync strategy is 'none'")
  {:tables #{}})

(defn- describe-database-explicit
  "Return the tables listed in the explicit schema configuration."
  [naming hide-foreign? database schema-config]
  (log/info "Using explicit schema configuration for database:" (:name database))
  (let [tables (cond->> (:tables schema-config)
                 hide-foreign? (remove #(uri/foreign-uri? (:name %) naming)))]
    {:tables (set (map #(build-table-from-config naming %) tables))}))

(defn- describe-database-auto
  "Discover the classes with the discovery query, capped by the database's
  class limit (default 100). A failed query logs and returns no tables."
  [database]
  (let [details       (:details database)
        naming        (uri/naming-context details)
        hide-foreign? (boolean (:hide-foreign-uris details))
        endpoint      (:endpoint details)
        options       {:insecure?     (:use-insecure details)
                       :default-graph (:default-graph details)
                       :auth          (auth/http-options details)}
        class-limit   (or (->long (:class-limit details)) 100)
        [success result] (execute/execute-sparql-query endpoint (templates/classes-discovery-query class-limit) options)]
    (if success
      (let [classes-with-counts (cond->> (get-in result [:results :bindings])
                                  :always (map (fn [binding]
                                                 {:uri   (get-in binding [:class :value])
                                                  :count (bigint (get-in binding [:count :value]))}))
                                  hide-foreign? (remove #(uri/foreign-uri? (:uri %) naming)))]
        {:tables (set (map #(build-table-from-sparql-result naming %) classes-with-counts))})
      (do
        (log/error "Error describing SPARQL database:" result)
        {:tables #{}}))))

(defn describe-database
  "Return `{:tables #{…}}` with the RDF classes of `database`, one table per
   class, found by its metadata sync strategy (auto, explicit, SHACL or none).

   An explicit strategy without a valid schema configuration falls back to
   auto."
  [_ database]
  (let [details       (:details database)
        sync-strategy (sync-strategy details)
        naming        (uri/naming-context details)
        hide-foreign? (boolean (:hide-foreign-uris details))
        schema-config (some-> details :schema-config parse-schema-config)]
    (cond
      (= sync-strategy :none)
      (describe-database-none)

      (= sync-strategy :shacl)
      (describe-database-shacl database)

      (and (= sync-strategy :explicit) schema-config)
      (describe-database-explicit naming hide-foreign? database schema-config)

      :else
      (describe-database-auto database))))