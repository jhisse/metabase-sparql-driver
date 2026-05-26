(ns metabase.driver.sparql.database
  "SPARQL Database for Metabase SPARQL Driver

   This namespace handles the discovery and description of \"tables\" (RDF classes)
   in SPARQL endpoints for Metabase."
  (:require [metabase.util.log :as log]
            [clojure.string :as str]
            [metabase.util.json :as json]
            [metabase.driver.sparql.auth :as auth]
            [metabase.driver.sparql.execute :as execute]
            [metabase.driver.sparql.shacl :as shacl]
            [metabase.driver.sparql.templates :as templates]
            [metabase.driver.sparql.uri :as uri]))

(defn- extract-class-name
  "Extracts the class name from a URI.
   
   Parameters:
     class-uri - RDF class URI

   Returns:
     Class name extracted from the last part of the URI (after the last '/' or '#')."
  [class-uri]
  (let [last-part (last (or (re-seq #"[^/#]+$" class-uri)
                            (re-seq #"[^/]+$" class-uri)
                            (re-seq #"[^#]+$" class-uri)
                            [class-uri]))]
    (if (str/blank? last-part)
      class-uri
      last-part)))

(defn- ->long
  "Coerce a manifest connection-property value to a Long (manifest props are
   type: string; legacy configs may hold a number). nil if blank/unparseable
   so callers can fall back to defaults.

   NOTE: this coercion only exists because Metabase's manifest spec
   (`build-drivers.lint-manifest-file/property-types`) rejects `integer`/`select`.
   If a future Metabase core accepts those types again, the limit/timeout fields
   can go back to `type: integer` and this helper can be removed."
  [v]
  (when (some? v)
    (parse-long (str/trim (str v)))))

(defn- parse-schema-config
  "Parses the schema configuration JSON string.
   Returns a map with a :tables key containing a list of table definitions, or nil if parsing fails or config is empty."
  [config-str]
  (when-not (str/blank? config-str)
    (try
      (json/decode+kw config-str)
      (catch Exception e
        (log/errorf "Error parsing schema configuration: %s" (.getMessage e))
        nil))))

(defn- build-pk-field
  "Creates the synthetic primary-key field that represents the RDF subject of each
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
  "Creates a field definition from a property URI.

   `default-graph` is stripped from the URI when it matches, so the column
   name in Metabase is the short local name (e.g. `naam`) instead of the
   full URI. The full URI is reconstructed at query-compile time."
  [default-graph idx field-uri]
  {:name (uri/shorten-uri field-uri default-graph)
   :database-type "string"
   :base-type :type/Text
   :pk? false
   :database-position (inc idx)})

(defn- build-fields-from-explicit-config
  "Builds field set from explicit schema configuration."
  [default-graph hide-foreign? explicit-table]
  (let [pk-field     (build-pk-field)
        candidates   (cond->> (:fields explicit-table)
                       hide-foreign? (remove #(uri/foreign-uri? % default-graph)))
        other-fields (map-indexed (partial build-field-from-uri default-graph) candidates)]
    (set (cons pk-field other-fields))))

(defn- build-fields-from-sparql-query
  "Builds field set from SPARQL query results."
  [default-graph hide-foreign? bindings]
  (let [pk-field   (build-pk-field)
        candidates (cond->> bindings
                     hide-foreign? (remove #(uri/foreign-uri? (get-in % [:property :value]) default-graph)))
        other-fields (map-indexed
                      (fn [idx binding]
                        (build-field-from-uri default-graph idx (get-in binding [:property :value])))
                      candidates)]
    (set (cons pk-field other-fields))))

(defn- describe-table-none
  "Handles describe-table when sync strategy is 'none'."
  [table]
  (log/info "Skipping table metadata sync for SPARQL database - sync strategy is 'none'")
  {:name (:name table)
   :schema nil
   :fields #{}})

(defn- describe-table-explicit
  "Handles describe-table when sync strategy is 'explicit'."
  [default-graph hide-foreign? table explicit-table]
  (log/info "Using explicit schema configuration for table:" (:name table))
  {:name (:name table)
   :schema nil
   :fields (build-fields-from-explicit-config default-graph hide-foreign? explicit-table)})

(defn- describe-table-auto
  "Handles describe-table when sync strategy is 'auto' (or fallback)."
  [database table]
  (let [details        (:details database)
        default-graph  (:default-graph details)
        hide-foreign?  (boolean (:hide-foreign-uris details))
        endpoint       (:endpoint details)
        options        {:insecure?     (:use-insecure details)
                        :default-graph default-graph
                        :auth          (auth/http-options details)}
        class-uri      (uri/absolute-uri (:name table) default-graph)
        property-limit (or (->long (:property-limit details)) 20)
        sample-limit   (or (->long (:sample-limit details)) 10000)
        query          (templates/class-properties-query class-uri property-limit sample-limit)
        [success result] (execute/execute-sparql-query endpoint query options)]
    (if success
      {:name (:name table)
       :schema nil
       :fields (build-fields-from-sparql-query default-graph hide-foreign? (get-in result [:results :bindings]))}
      (do
        (log/error "Error describing SPARQL table:" result)
        {:fields #{}}))))

;; ---- SHACL-driven sync ------------------------------------------------------

(defn- shacl-prop->field
  "Convert one SHACL property descriptor into a Metabase TableMetadataField."
  [default-graph hide-foreign? idx prop]
  (let [uri      (:property-uri prop)
        foreign? (uri/foreign-uri? uri default-graph)]
    (when-not (and foreign? hide-foreign?)
      (cond-> {:name              (uri/shorten-uri uri default-graph)
               :database-type     (if (:lang-string? prop) "langString" "string")
               :base-type         (or (:base-type prop) :type/Text)
               :pk?               false
               :database-position (inc idx)}
        (:semantic-type prop)     (assoc :semantic-type (:semantic-type prop))
        (:description prop)       (assoc :field-comment (:description prop))
        (:database-required prop) (assoc :database-required true)))))

(defn- shacl-shape->table
  "Convert one SHACL shape into a Metabase TableMetadata `:table` entry."
  [default-graph {:keys [class-uri description]}]
  {:name         (uri/shorten-uri class-uri default-graph)
   :schema       nil
   :display-name (extract-class-name class-uri)
   :description  (or description (str "RDF Class: " class-uri " (SHACL)"))})

(defn- shacl-shape->describe-table
  "Convert one SHACL shape into the map returned by `driver/describe-table`.

   Properties are emitted in `sh:order` ascending, with `:property-uri` as a
   tie-breaker so the output is deterministic; properties without `sh:order`
   sort to the end."
  [default-graph hide-foreign? {:keys [class-uri properties]}]
  (let [pk-field   (build-pk-field)
        candidates (cond->> properties
                     hide-foreign? (remove #(uri/foreign-uri? (:property-uri %) default-graph))
                     :always       (sort-by (juxt #(or (:order %) Long/MAX_VALUE)
                                                  :property-uri)))
        fields     (->> candidates
                        (map-indexed (fn [idx p] (shacl-prop->field default-graph hide-foreign? idx p)))
                        (remove nil?))]
    {:name   (uri/shorten-uri class-uri default-graph)
     :schema nil
     :fields (set (cons pk-field fields))}))

(defn- shape-for-table
  "Find the SHACL shape whose class matches `table` (after URI reconstruction)."
  [shapes default-graph table]
  (let [full (uri/absolute-uri (:name table) default-graph)]
    (some #(when (= (:class-uri %) full) %) shapes)))

(defn- shacl-fetch-opts
  "Build the HTTP options map for the SHACL fetch from connection `details`.
   Timeouts are configured in seconds and the size cap in megabytes; unset
   values are left `nil` so the SHACL extractor applies its own defaults."
  [details]
  {:connect-timeout-ms (some-> (:shacl-connect-timeout details) ->long (* 1000))
   :socket-timeout-ms  (some-> (:shacl-socket-timeout details) ->long (* 1000))
   :max-bytes          (some-> (:shacl-max-size-mb details) ->long (* 1024 1024))})

(defn- shacl-shapes
  "Fetch and cache SHACL shapes for `database`. Returns `nil` if no URL is
   configured (we treat this as a misconfiguration and let the caller error).
   Language preference (for `sh:name`/`sh:description`) and the HTTP
   timeout/size-cap settings are read from the connection details and
   forwarded to the SHACL extractor."
  [database]
  (when-let [url (-> database :details :shacl-url)]
    (let [details (:details database)
          lang    (or (:default-language details) "")
          opts    (shacl-fetch-opts details)]
      (try
        (shacl/metadata url lang opts)
        (catch Exception t
          (log/errorf t "[shacl] Failed to load SHACL document at %s" url)
          nil)))))

(defn fks
  "Return the FK rows for `describe-fks` derived from the SHACL document
   configured on `database`. Returns an empty seq for non-SHACL sync strategies."
  [database]
  (if-not (= :shacl (keyword (get-in database [:details :metadata-sync-strategy] "auto")))
    []
    (let [default-graph (-> database :details :default-graph)
          hide-foreign? (boolean (-> database :details :hide-foreign-uris))
          shapes        (shacl-shapes database)]
      (for [shape shapes
            prop  (:properties shape)
            :let  [fk-class (:fk-target-class prop)
                   prop-uri (:property-uri prop)]
            :when fk-class
            :when (not (and hide-foreign?
                            (or (uri/foreign-uri? fk-class default-graph)
                                (uri/foreign-uri? prop-uri default-graph)
                                (uri/foreign-uri? (:class-uri shape) default-graph))))]
        {:fk-table-name   (uri/shorten-uri (:class-uri shape) default-graph)
         :fk-table-schema nil
         :fk-column-name  (uri/shorten-uri prop-uri default-graph)
         :pk-table-name   (uri/shorten-uri fk-class default-graph)
         :pk-table-schema nil
         :pk-column-name  "subject"}))))

(defn- describe-database-shacl
  [database]
  (let [details       (:details database)
        default-graph (:default-graph details)
        hide-foreign? (boolean (:hide-foreign-uris details))
        shapes        (shacl-shapes database)]
    (when-not shapes
      (log/warnf "[shacl] No shapes available for database %s; returning empty table set" (:name database)))
    {:tables (->> (or shapes [])
                  (remove (fn [s] (and hide-foreign?
                                       (uri/foreign-uri? (:class-uri s) default-graph))))
                  (map #(shacl-shape->table default-graph %))
                  set)}))

(defn- describe-table-shacl
  [database table]
  (let [details       (:details database)
        default-graph (:default-graph details)
        hide-foreign? (boolean (:hide-foreign-uris details))
        shapes        (shacl-shapes database)
        match         (shape-for-table shapes default-graph table)]
    (if match
      (shacl-shape->describe-table default-graph hide-foreign? match)
      (do
        (log/warnf "[shacl] No shape found for table %s; returning empty fields"
                   (:name table))
        {:name (:name table) :schema nil :fields #{(build-pk-field)}}))))

(defn describe-table
  "Describes the fields (properties) of an RDF class (SPARQL table).

   Parameters:
     _ - driver (not used)
     database - Metabase Database instance
     table - Table definition with :name containing the (possibly shortened) RDF class URI

   Returns:
     Map with :name, :schema, and :fields keys describing the table structure"
  [_ database table]
  (let [details        (:details database)
        sync-strategy  (keyword (get details :metadata-sync-strategy "auto"))
        default-graph  (:default-graph details)
        hide-foreign?  (boolean (:hide-foreign-uris details))
        schema-config  (some-> details :schema-config parse-schema-config)
        full-name      (uri/absolute-uri (:name table) default-graph)
        explicit-table (when (= sync-strategy :explicit)
                         (some #(when (= (:name %) full-name) %) (:tables schema-config)))]
    (cond
      (= sync-strategy :none)
      (describe-table-none table)

      (= sync-strategy :shacl)
      (describe-table-shacl database table)

      (and (= sync-strategy :explicit) explicit-table)
      (describe-table-explicit default-graph hide-foreign? table explicit-table)

      :else
      (describe-table-auto database table))))

(defn- build-table-from-config
  "Builds a table definition from schema configuration."
  [default-graph table]
  (let [uri        (:name table)
        short-name (uri/shorten-uri uri default-graph)]
    {:name short-name
     :schema nil
     :display-name (extract-class-name uri)
     :description (or (:description table)
                      (str "RDF Class: " uri " (Explicit)"))}))

(defn- build-table-from-sparql-result
  "Builds a table definition from SPARQL query results."
  [default-graph {:keys [uri count]}]
  {:name (uri/shorten-uri uri default-graph)
   :schema nil
   :display-name (extract-class-name uri)
   :description (str "RDF Class: " uri " (Instances: " count ")")})

(defn- describe-database-none
  "Handles describe-database when sync strategy is 'none'."
  []
  (log/info "Skipping metadata sync for SPARQL database - sync strategy is 'none'")
  {:tables #{}})

(defn- describe-database-explicit
  "Handles describe-database when sync strategy is 'explicit'."
  [default-graph hide-foreign? database schema-config]
  (log/info "Using explicit schema configuration for database:" (:name database))
  (let [tables (cond->> (:tables schema-config)
                 hide-foreign? (remove #(uri/foreign-uri? (:name %) default-graph)))]
    {:tables (set (map #(build-table-from-config default-graph %) tables))}))

(defn- describe-database-auto
  "Handles describe-database when sync strategy is 'auto' (or fallback)."
  [database]
  (let [details       (:details database)
        default-graph (:default-graph details)
        hide-foreign? (boolean (:hide-foreign-uris details))
        endpoint      (:endpoint details)
        options       {:insecure?     (:use-insecure details)
                       :default-graph default-graph
                       :auth          (auth/http-options details)}
        class-limit   (or (->long (:class-limit details)) 100)
        [success result] (execute/execute-sparql-query endpoint (templates/classes-discovery-query class-limit) options)]
    (if success
      (let [classes-with-counts (cond->> (get-in result [:results :bindings])
                                  :always (map (fn [binding]
                                                 {:uri   (get-in binding [:class :value])
                                                  :count (bigint (get-in binding [:count :value]))}))
                                  hide-foreign? (remove #(uri/foreign-uri? (:uri %) default-graph)))]
        {:tables (set (map #(build-table-from-sparql-result default-graph %) classes-with-counts))})
      (do
        (log/error "Error describing SPARQL database:" result)
        {:tables #{}}))))

(defn describe-database
  "Discovers the available 'tables' (RDF classes) in the SPARQL endpoint.

   Parameters:
     _ - driver (not used)
     database - Metabase Database instance

   Returns:
     Map with the :tables key containing a set of table definitions."
  [_ database]
  (let [details       (:details database)
        sync-strategy (keyword (get details :metadata-sync-strategy "auto"))
        default-graph (:default-graph details)
        hide-foreign? (boolean (:hide-foreign-uris details))
        schema-config (some-> details :schema-config parse-schema-config)]
    (cond
      (= sync-strategy :none)
      (describe-database-none)

      (= sync-strategy :shacl)
      (describe-database-shacl database)

      (and (= sync-strategy :explicit) schema-config)
      (describe-database-explicit default-graph hide-foreign? database schema-config)

      :else
      (describe-database-auto database))))