;; SPARQL Connection for Metabase SPARQL Driver
;;
;; This namespace manages connections to SPARQL endpoints.
;; Provides functions to test connectivity and manage connection details.
(ns metabase.driver.sparql.connection
  (:require [metabase.util.log :as log]
            [metabase.driver.sparql.execute :as execute]
            [metabase.driver.sparql.templates :as templates]))

(defn can-connect?
  "Checks if a connection to a SPARQL endpoint can be established.
   
   Parameters:
     endpoint - SPARQL endpoint URL
     options - Map of additional options:
       :default-graph - URI of the default graph (optional)
       :insecure? - Flag to ignore SSL certificate validation (optional, derived from :use-insecure)
   
   Returns:
     true if the connection is successful, false otherwise."
  [endpoint options]
  (log/info "Trying to connect to SPARQL endpoint:" endpoint)
  (let [[success _] (execute/execute-sparql-query endpoint (templates/connection-test-query) options)]
    success))
