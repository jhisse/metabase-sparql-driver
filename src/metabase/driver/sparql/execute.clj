;; SPARQL Query Execution for Metabase SPARQL Driver
;;
;; This namespace handles the execution of SPARQL queries against endpoints.
;; It provides functions for sending HTTP requests, handling responses,
;; and processing errors when communicating with SPARQL endpoints.
(ns metabase.driver.sparql.execute
  (:require [clojure.tools.logging :as log]
            [clj-http.client :as http]
            [metabase.util.json :as json]))

(def ^:private default-accept-header "application/json")

;; Execute SPARQL queries against an endpoint
;;
;; Parameters:
;;   endpoint - URL of the SPARQL endpoint
;;   query - SPARQL query string to execute
;;   options - Map of additional options:
;;     :default-graph - URI of the default graph to query (optional)
;;     :insecure? - Boolean flag to ignore SSL certificate validation (optional)
;;
;; Returns:
;;   On success: [true, response-body] where response-body is the parsed JSON response
;;   On failure: [false, error-message] with the error message as string
;;
;; This function handles all HTTP communication with the SPARQL endpoint,
;; including error handling, logging, and response parsing.
(defn execute-sparql-query
  [endpoint query options]
  (try
    (let [params (cond-> {:query query}
                   (:default-graph options) (assoc :default-graph-uri (:default-graph options)))
          http-options (cond-> {:query-params params
                                :headers {"Accept" default-accept-header}
                                :throw-exceptions false}
                         (:insecure? options) (assoc :insecure? true))]
      (let [response (http/get endpoint http-options)]
        (if (= 200 (:status response))
          (let [body (json/decode+kw (:body response))]
            [true body])
          [false (str "SPARQL endpoint returned status: " (:status response) "\nBody: " (:body response))])))
    (catch Exception e
      (log/error "Error executing SPARQL query:" (.getMessage e))
      [false (.getMessage e)])))
