;; SPARQL Database for Metabase SPARQL Driver
;;
;; This namespace handles the discovery and description of "tables" (RDF classes)
;; in SPARQL endpoints for Metabase.
(ns metabase.driver.sparql.database
  (:require [clojure.tools.logging :as log]
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

(defn describe-database
  "Discovers the available 'tables' (RDF classes) in the SPARQL endpoint.
   
   Parameters:
     endpoint - SPARQL endpoint URL
     options - Map of additional options including :insecure? (derived from :use-insecure)
   
   Returns:
     Map with key :tables containing a set of table definitions."
  [endpoint options]
  (let [[success result] (execute/execute-sparql-query endpoint (templates/classes-discovery-query) options)]
    (if success
      (let [classes-with-counts (map (fn [binding]
                                       {:uri (get-in binding [:class :value])
                                        :count (Integer/parseInt (get-in binding [:count :value]))})
                                     (get-in result [:results :bindings]))]
        {:tables
         (set
          (for [{:keys [uri count]} classes-with-counts]
            {:name (extract-class-name uri)
             :schema nil
             :display-name (extract-class-name uri)
             :description (str "RDF Class: " uri " (Instances: " count ")")
             :fields nil}))})
      (do
        (log/error "Error describing SPARQL database:" result)
        {:tables #{}}))))
