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

;; Execute SPARQL queries against an endpoint using POST
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
;; This function handles all HTTP communication with the SPARQL endpoint
;; using the POST method, which is robust for queries of any length.
(defn execute-sparql-query
  [endpoint query options]
  (try
    (let [;; Options for the POST request
          http-options (cond-> {:headers          {"Accept" default-accept-header}
                                :throw-exceptions false
                                :form-params      {:query query}}
                         (:insecure? options) (assoc :insecure? true))

          ;; The default-graph-uri is sent as a query parameter, even in a POST request
          http-options (if-let [dg (:default-graph options)]
                         (assoc http-options :query-params {:default-graph-uri dg})
                         http-options)

          response (http/post endpoint http-options)]

      (if (= 200 (:status response))
        (let [body (json/decode+kw (:body response))]
          [true body])
        [false (str "SPARQL endpoint returned status: " (:status response) "\nBody: " (:body response))]))
    (catch Exception e
      (log/error "Error executing SPARQL query:" (.getMessage e))
      [false (.getMessage e)])))
