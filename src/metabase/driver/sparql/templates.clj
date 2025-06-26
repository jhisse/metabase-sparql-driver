;; SPARQL Queries for Metabase SPARQL Driver
;;
;; This namespace contains predefined SPARQL queries used by the driver
;; for various operations such as connection testing and table discovery.
;; Each query is optimized for specific use cases.
(ns metabase.driver.sparql.templates)

;; Simple query to test connectivity with a SPARQL endpoint
;;
;; Returns:
;;   A single row with a single column named ?ping containing the value 'pong'
;;
;; Usage:
;;   Used by the driver/can-connect? method to check endpoint availability
;;   A successful execution of this query indicates the endpoint is accessible
(defn connection-test-query []
  "SELECT ?ping 
   WHERE { 
     BIND('pong' AS ?ping) 
   }")

;; Query to discover RDF classes in the endpoint
;;
;; Returns:
;;   A list of RDF classes (?class) with their instance counts (?count)
;;   Limited to the top 100 classes by instance count
;;
;; Usage:
;;   Used by the driver/describe-database method to discover available "tables"
;;   Each RDF class is treated as a table in Metabase's data model
;;   The count helps identify the most significant classes in the dataset
(defn classes-discovery-query []
  "SELECT ?class (COUNT(?s) AS ?count) 
   WHERE { 
     ?s a ?class 
   } 
   GROUP BY ?class 
   ORDER BY DESC(?count) 
   LIMIT 100")
