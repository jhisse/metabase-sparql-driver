(ns metabase.driver.sparql.execute
  "SPARQL Query Execution for Metabase SPARQL Driver

   This namespace provides functions to execute SPARQL queries via HTTP, handle responses, and process errors."
  (:require [metabase.util.log :as log]
            [clj-http.client :as http]
            [metabase.util.json :as json]
            [metabase.query-processor.store :as qp.store]
            [metabase.lib.metadata :as lib.metadata]
            [metabase.driver.sparql.query-processor :as query-processor]))

(def ^:private default-accept-header "application/json")

(defn execute-sparql-query
  "Execute SPARQL queries against an endpoint using POST.

   Parameters:
     endpoint - URL of the SPARQL endpoint
     query - SPARQL query string to execute
     options - Map of additional options:
       :default-graph - URI of the default graph to query (optional)
       :insecure? - Boolean flag to ignore SSL certificate validation (optional)

   Returns:
     On success: [true, response-body] where response-body is the parsed JSON response
     On failure: [false, error-message] with the error message as string

   This function handles all HTTP communication with the SPARQL endpoint
   using the POST method, which is robust for queries of any length."
  [endpoint query options]
  (try
    (let [http-options (cond-> {:headers          {"Accept" default-accept-header}
                                :throw-exceptions false
                                :form-params      {:query query}}
                         (:insecure? options) (assoc :insecure? true))

          ;; Send default-graph-uri as a query parameter if provided
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

(defn execute-reducible-query
  "Executes a SPARQL query and processes the results for Metabase.
  
  Arguments:
    - native-query: map containing at least :query and optionally :endpoint under :native
    - respond: callback function to handle the processed results

  This function retrieves database details, executes the SPARQL query, and processes the response.
  On failure, it logs the error and returns an empty columns result."
  [native-query _context respond]
  (log/info "Executing SPARQL query:" (pr-str (select-keys native-query [:native])))
  (let [database (lib.metadata/database (qp.store/metadata-provider))
        endpoint (or (get-in native-query [:native :endpoint]) (-> database :details :endpoint))
        sparql-query (get-in native-query [:native :query])
        options {:default-graph (-> database :details :default-graph)
                 :insecure? (-> database :details :use-insecure)}
        [success result] (execute-sparql-query endpoint sparql-query options)]
    (if success
      (query-processor/process-query-results result respond)
      (do
        (log/error "Error executing SPARQL query:" result)
        (respond {:cols []} [])))))

