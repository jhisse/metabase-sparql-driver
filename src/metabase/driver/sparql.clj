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

;; Private function to execute SPARQL queries against an endpoint
;;
;; Parameters:
;;   endpoint - URL of the SPARQL endpoint
;;   query - SPARQL query string to execute
;;   options - Map of additional options:
;;     :default-graph - URI of the default graph to query (optional)
;;     :insecure - Boolean flag to ignore SSL certificate validation (optional)
;;
;; Returns:
;;   On success: [true, response-body] where response-body is the parsed JSON response
;;   On failure: [false, error-message] with the error message as string
;;
;; This function handles all HTTP communication with the SPARQL endpoint,
;; including error handling, logging, and response parsing.
(defn- execute-sparql-query
  [endpoint query {:keys [default-graph insecure?]}]
  (try
    (let [params (cond-> {:query query}
                   default-graph (assoc :default-graph-uri default-graph))
          http-options (cond-> {:query-params params
                                :headers {"Accept" "application/json"}
                                :throw-exceptions false}
                         insecure? (assoc :insecure? true))]
      (when insecure?
        (log/warn "Using insecure connection (ignoring SSL certificate validation)"))

      (let [response (http/get endpoint http-options)]
        (if (= 200 (:status response))
          (let [body (json/parse-string (:body response) true)]
            [true body])
          [false (str "SPARQL endpoint returned status: " (:status response))])))
    (catch Exception e
      (log/error "Error executing SPARQL query:" (.getMessage e))
      [false (.getMessage e)])))

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
  (log/info "Attempting to connect to SPARQL endpoint:" (:endpoint details))

  (let [endpoint (:endpoint details)
        query "SELECT ?ping WHERE { BIND('pong' AS ?ping) }"
        options {:default-graph (:default-graph details)
                 :insecure? (:insecure details)}
        [success _] (execute-sparql-query endpoint query options)]
    success))

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
  (let [endpoint (-> database :details :endpoint)
        ;; SPARQL query to get all distinct classes in the endpoint
        query "SELECT ?class (COUNT(?s) AS ?count) WHERE { ?s a ?class } GROUP BY ?class ORDER BY DESC(?count) LIMIT 100"
        options {:insecure? (-> database :details :insecure)}
        [success result] (execute-sparql-query endpoint query options)]

    (if success
      (let [;; Extract class URIs and instance counts from the SPARQL JSON response
            ;; Each binding contains :class and :count keys with their respective values
            classes-with-counts (map (fn [binding]
                                       {:uri (get-in binding [:class :value])
                                        :count (Integer/parseInt (get-in binding [:count :value]))})
                                     (get-in result [:results :bindings]))]
        {:tables
         (set
          (for [{:keys [uri count]} classes-with-counts
                :let [class-name (last (str/split uri #"[/#]"))]]
            {:name         class-name
             :schema       nil
             :display_name class-name
             :description  (str "Class with " count " instances")}))})
      (do
        (log/error "Error describing SPARQL database:" result)
        {:tables #{}}))))
