(ns metabase.driver.sparql.database
  "SPARQL Database for Metabase SPARQL Driver

   This namespace handles the discovery and description of \"tables\" (RDF classes)
   in SPARQL endpoints for Metabase."
  (:require [metabase.util.log :as log]
            [clojure.string :as str]
            [metabase.util.json :as json]
            [metabase.driver.sparql.execute :as execute]
            [metabase.driver.sparql.templates :as templates]))

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
  "Creates the primary key field (id) for a table."
  []
  {:name "id"
   :database-type "uri"
   :base-type :type/Text
   :pk? true
   :database-position 0})

(defn- build-field-from-uri
  "Creates a field definition from a property URI."
  [idx field-uri]
  {:name field-uri
   :database-type "string"
   :base-type :type/Text
   :pk? false
   :database-position (inc idx)})

(defn- build-fields-from-explicit-config
  "Builds field set from explicit schema configuration."
  [explicit-table]
  (let [pk-field (build-pk-field)
        other-fields (map-indexed build-field-from-uri (:fields explicit-table))]
    (set (cons pk-field other-fields))))

(defn- build-fields-from-sparql-query
  "Builds field set from SPARQL query results."
  [bindings]
  (let [pk-field (build-pk-field)
        other-fields (map-indexed
                      (fn [idx binding]
                        (build-field-from-uri idx (get-in binding [:property :value])))
                      bindings)]
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
  [table explicit-table]
  (log/info "Using explicit schema configuration for table:" (:name table))
  {:name (:name table)
   :schema nil
   :fields (build-fields-from-explicit-config explicit-table)})

(defn- describe-table-auto
  "Handles describe-table when sync strategy is 'auto' (or fallback)."
  [database table]
  (let [endpoint (-> database :details :endpoint)
        options {:insecure? (-> database :details :use-insecure)
                 :default-graph (-> database :details :default-graph)}
        class-uri (:name table)
        query (templates/class-properties-query class-uri)
        [success result] (execute/execute-sparql-query endpoint query options)]
    (if success
      {:name (:name table)
       :schema nil
       :fields (build-fields-from-sparql-query (get-in result [:results :bindings]))}
      (do
        (log/error "Error describing SPARQL table:" result)
        {:fields #{}}))))

(defn describe-table
  "Describes the fields (properties) of an RDF class (SPARQL table).

   Parameters:
     _ - driver (not used)
     database - Metabase Database instance
     table - Table definition with :name containing the RDF class URI
   
   Returns:
     Map with :name, :schema, and :fields keys describing the table structure"
  [_ database table]
  (let [sync-strategy (keyword (get-in database [:details :metadata-sync-strategy] "auto"))
        schema-config (some-> database :details :schema-config parse-schema-config)
        explicit-table (when (= sync-strategy :explicit)
                         (some #(when (= (:name %) (:name table)) %) (:tables schema-config)))]
    (cond
      (= sync-strategy :none)
      (describe-table-none table)

      (and (= sync-strategy :explicit) explicit-table)
      (describe-table-explicit table explicit-table)

      :else
      (describe-table-auto database table))))

(defn- build-table-from-config
  "Builds a table definition from schema configuration."
  [table]
  {:name (:name table)
   :schema nil
   :display-name (extract-class-name (:name table))
   :description (or (:description table)
                    (str "RDF Class: " (:name table) " (Explicit)"))})

(defn- build-table-from-sparql-result
  "Builds a table definition from SPARQL query results."
  [{:keys [uri count]}]
  {:name uri
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
  [database schema-config]
  (log/info "Using explicit schema configuration for database:" (:name database))
  {:tables (set (map build-table-from-config (:tables schema-config)))})

(defn- describe-database-auto
  "Handles describe-database when sync strategy is 'auto' (or fallback)."
  [database]
  (let [endpoint (-> database :details :endpoint)
        options {:insecure? (-> database :details :use-insecure)
                 :default-graph (-> database :details :default-graph)}
        [success result] (execute/execute-sparql-query endpoint (templates/classes-discovery-query 20) options)]
    (if success
      (let [classes-with-counts (map (fn [binding]
                                       {:uri (get-in binding [:class :value])
                                        :count (bigint (get-in binding [:count :value]))})
                                     (get-in result [:results :bindings]))]
        {:tables (set (map build-table-from-sparql-result classes-with-counts))})
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
  (let [sync-strategy (keyword (get-in database [:details :metadata-sync-strategy] "auto"))
        schema-config (some-> database :details :schema-config parse-schema-config)]
    (cond
      (= sync-strategy :none)
      (describe-database-none)

      (and (= sync-strategy :explicit) schema-config)
      (describe-database-explicit database schema-config)

      :else
      (describe-database-auto database))))