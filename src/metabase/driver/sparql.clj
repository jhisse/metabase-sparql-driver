;; SPARQL Driver for Metabase
;;
;; This driver enables Metabase to connect to SPARQL endpoints for querying RDF data.
;; It implements a custom approach (not based on SQL/JDBC) using HTTP requests
;; to communicate with SPARQL endpoints. The driver supports both secure and insecure
;; connections with optional default graph specification.
(ns metabase.driver.sparql
  (:require [clojure.tools.logging :as log]
            [metabase.driver :as driver]
            [clj-http.client :as http]
            [cheshire.core :as json]
            [clojure.string :as str]))

;; Register SPARQL driver with Metabase's driver system
;; No parent sql or sql-jdbc because it is a custom driver using clj-http
;; This registration makes the driver available to Metabase for database connections
(driver/register! :sparql)

;; Method to test connection to SPARQL endpoint
;; This method verifies if Metabase can successfully connect to the specified SPARQL endpoint
;;
;; Parameters:
;;   _ - Driver instance (ignored)
;;   details - A map containing connection details with the following keys:
;;     :endpoint - URL of the SPARQL endpoint (required)
;;     :default-graph - URI of the default graph to query (optional)
;;     :insecure - Boolean flag to ignore SSL certificate validation (optional)
;;
;; Returns:
;;   Boolean - true if connection successful (HTTP 200 response), false otherwise
;;
;; Behavior:
;;   1. Executes a simple SPARQL query to test connectivity
;;   2. Logs warnings if using insecure connection
;;   3. Returns false and logs error on any exception
(defmethod driver/can-connect? :sparql
  [_ details]
  (try
    (log/info "Attempting to connect to SPARQL endpoint:" (:endpoint details))
    
    (let [endpoint (:endpoint details)
          default-graph (:default-graph details)
          insecure? (:insecure details)
          query "SELECT ?noop WHERE {BIND('noop' as ?noop)}"
          params (cond-> {:query query}
                   default-graph (assoc :default-graph-uri default-graph))
          http-options (cond-> {:query-params params
                               :headers {"Accept" "application/json"}
                               :throw-exceptions false}
                        insecure? (assoc :insecure? true))
          _ (when insecure?
              (log/warn "Using insecure connection (ignoring SSL certificate validation)"))
          response (http/get endpoint http-options)]
      
      (= 200 (:status response)))
    
    (catch Exception e
      (log/error "Error connecting to SPARQL endpoint:" (.getMessage e))
      false)))

;; Implementation of describe-database method for SPARQL driver
;; This method discovers the available "tables" (RDF classes) in the SPARQL endpoint
;;
;; Parameters:
;;   _ - Driver instance (ignored)
;;   database - Database instance containing connection details
;;
;; Returns:
;;   A map with :tables key containing a set of table definitions, where each table
;;   represents an RDF class found in the endpoint
;;
;; Behavior:
;;   1. Queries the endpoint for distinct RDF classes (limited to 100)
;;   2. Extracts the class URIs from the response
;;   3. Converts each class URI to a table definition by extracting the class name
;;      from the last part of the URI (after the last '/' or '#')
;;   4. Returns empty table set and logs error on any exception
(defmethod driver/describe-database :sparql
  [_ database]
  (try
    (let [endpoint (-> database :details :endpoint)
          insecure? (-> database :details :insecure)
          ;; SPARQL query to get all distinct classes in the endpoint
          query "SELECT DISTINCT ?class WHERE { ?s a ?class } LIMIT 100"
          http-options (cond-> {:query-params {:query query}
                               :headers {"Accept" "application/json"}
                               :throw-exceptions false}
                        insecure? (assoc :insecure? true))
          _ (when insecure?
              (log/warn "Using insecure connection (ignoring SSL certificate validation)"))
          response (http/get endpoint http-options)
          body (json/parse-string (:body response) true)
          ;; Extract class URIs from the SPARQL JSON response
          ;; Each binding contains a :class key with a :value that holds the class URI
          classes (map (fn [binding] 
                         (get-in binding [:class :value]))
                       (get-in body [:results :bindings]))]
      {:tables
       (set
        (for [class-uri classes
              :let [class-name (last (str/split class-uri #"[/#]"))]]
          {:name   class-name
           :schema nil
           :display_name class-name}))})
    (catch Exception e
      (log/error "Error describing SPARQL database:" (.getMessage e))
      {:tables #{}})))
