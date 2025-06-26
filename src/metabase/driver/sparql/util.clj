;; SPARQL Utilities for Metabase SPARQL Driver
;;
;; This namespace provides utility functions for the SPARQL driver.
;; Includes functions to extract connection details and other helper operations.
(ns metabase.driver.sparql.util)

(defn extract-endpoint-details
  "Extracts SPARQL endpoint details from a database or query object.
   
   Parameters:
     database-or-query - Database or query object
   Returns:
     Map containing :endpoint, :default-graph, and :use-insecure?"
  [database-or-query]
  (let [details (or (:details database-or-query)
                    (get-in database-or-query [:database :details]))]
    {:endpoint (:endpoint details)
     :default-graph (:default-graph details)
     :insecure? (:use-insecure details)}))
