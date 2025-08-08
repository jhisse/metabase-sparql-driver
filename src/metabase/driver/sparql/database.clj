(ns metabase.driver.sparql.database
  "SPARQL Database for Metabase SPARQL Driver

   This namespace handles the discovery and description of \"tables\" (RDF classes)
   in SPARQL endpoints for Metabase."
  (:require [metabase.util.log :as log]
            [clojure.string :as str]
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

(defn describe-table
  "Describes the fields (properties) of an RDF class (SPARQL table).

   Parameters:
     _ - driver (not used)
     database - Metabase Database instance
     table - Table definition with :name containing the RDF class URI
   
   Returns:
     Map with :name, :schema, and :fields keys describing the table structure"
  [_ database table]
  ;; Check if metadata sync is disabled
  (if (-> database :details :dont-sync-metadata)
    (do
      (log/info "Skipping table metadata sync for SPARQL database - dont-sync-metadata is enabled")
      {:name   (:name table)
       :schema nil
       :fields #{}})
    (let [endpoint (-> database :details :endpoint)
          options {:insecure? (-> database :details :use-insecure)
                   :default-graph (-> database :details :default-graph)}
          class-uri (:name table)
          query (templates/class-properties-query class-uri)
          [success result] (execute/execute-sparql-query endpoint query options)]
      (if success
        (let [bindings (get-in result [:results :bindings])
              pk-field {:name "id", :database-type "uri", :base-type :type/Text, :pk? true, :database-position 0}
              other-fields (map-indexed
                            (fn [idx binding]
                              (let [property-uri (get-in binding [:property :value])]
                                {:name property-uri
                                 :database-type "string"
                                 :base-type :type/Text
                                 :pk? false
                                 :database-position (inc idx)}))
                            bindings)]
          {:name   (:name table)
           :schema nil
           :fields (set (cons pk-field other-fields))})
        (do
          (log/error "Error describing SPARQL table:" result)
          {:fields #{}})))))

(defn describe-database
  "Discovers the available 'tables' (RDF classes) in the SPARQL endpoint.

   Parameters:
     _ - driver (not used)
     database - Metabase Database instance

   Returns:
     Map with the :tables key containing a set of table definitions."
  [_ database]
  ;; Check if metadata sync is disabled
  (if (-> database :details :dont-sync-metadata)
    (do
      (log/info "Skipping metadata sync for SPARQL database - dont-sync-metadata is enabled")
      {:tables #{}})
    (let [endpoint (-> database :details :endpoint)
          options {:insecure? (-> database :details :use-insecure)
                   :default-graph (-> database :details :default-graph)}
          [success result] (execute/execute-sparql-query endpoint (templates/classes-discovery-query 20) options)]
      (if success
        (let [classes-with-counts (map (fn [binding]
                                         {:uri (get-in binding [:class :value])
                                          :count (bigint (get-in binding [:count :value]))})
                                       (get-in result [:results :bindings]))]
          {:tables
           (set
            (for [{:keys [uri count]} classes-with-counts]
              {:name uri
               :schema nil
               :display-name (extract-class-name uri)
               :description (str "RDF Class: " uri " (Instances: " count ")")}))})
        (do
          (log/error "Error describing SPARQL database:" result)
          {:tables #{}})))))