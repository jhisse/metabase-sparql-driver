(ns metabase.driver.sparql.connection
  "SPARQL Connection for Metabase SPARQL Driver

   This namespace manages connections to SPARQL endpoints.
   Provides functions to test connectivity and manage connection details."
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

(defn dbms-version
  "Checks and returns the version of the SPARQL endpoint.

   Parameters:
     _driver   - Driver instance (not used)
     database  - Metabase Database instance

   Returns:
     A map with the key :version and the detected version as value, e.g. {:version \"SPARQL 1.1\"}.
     If the version cannot be determined, returns {:version \"SPARQL 1.0\"}."
  [_driver database]
  (let [endpoint         (-> database :details :endpoint)
        options          {:default-graph (-> database :details :default-graph)
                          :insecure?    (-> database :details :use-insecure)}
        [ok-bind res-bind]     (execute/execute-sparql-query endpoint (templates/sparql-1-1-bind-version-query) options)
        [ok-values res-values] (execute/execute-sparql-query endpoint (templates/sparql-1-1-values-version-query) options)
        result  (cond
                  ok-bind   res-bind
                  ok-values res-values
                  :else     nil)]
    (if result
      (let [bindings (get-in result [:results :bindings])]
        (log/debugf "SPARQL endpoint supports 1.1 features.")
        {:version (get-in bindings [:version :value])})
      (do
        (log/errorf "Error getting SPARQL version: [Bind] %s" res-bind)
        (log/errorf "Error getting SPARQL version: [Values] %s" res-values)
        {:version "SPARQL 1.0"})))) ;; Default to SPARQL 1.0 if no version is detected (or would "unknown" be better?)