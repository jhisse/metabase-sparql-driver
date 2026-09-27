(ns metabase.driver.sparql.execute
  "POST SPARQL queries to the endpoint, decode the JSON response, and classify
   failures so Metabase reports them as query or database errors."
  (:require [metabase.util.log :as log]
            [clj-http.client :as http]
            [metabase.util.json :as json]
            [metabase.driver-api.core :as driver-api]
            [metabase.driver.sparql.auth :as auth]
            [metabase.driver.sparql.uri :as uri]
            [metabase.driver.sparql.query-processor :as query-processor]))

(def ^:private user-agent
  "Sent with every query. Wikimedia endpoints (query.wikidata.org) answer 403
   to the HTTP client's default User-Agent."
  "metabase-sparql-driver (https://github.com/jhisse/metabase-sparql-driver)")

(defn- ^:private create-http-options
  "Build the clj-http options that POST `query` as a form parameter.

   `options` takes `:insecure?` (skip TLS certificate checks),
   `:default-graph` (sent as `default-graph-uri`) and `:auth` (from
   `[[auth/http-options]]`, merged in)."
  [query {:keys [insecure? default-graph auth]}]
  (cond-> {:accept :json
           :cookie-policy :none
           :throw-exceptions false
           :form-params {:query query}
           :headers {"User-Agent" user-agent}}
    insecure? (assoc :insecure? true)
    default-graph (assoc :query-params {:default-graph-uri default-graph})
    (seq auth) (#(merge-with merge % auth))))

(defn- ^:private parse-json-response
  "Decode the JSON body of `response`, or return nil and log the error when
   it is not valid JSON."
  [response]
  (try
    (json/decode+kw (:body response))
    (catch Exception json-e
      (log/errorf "Error parsing JSON response: %s" (.getMessage json-e))
      (log/debugf "Full body response: %s" (:body response))
      nil)))

(def ^:private max-error-body-chars
  "Cap on how much of an endpoint error body is embedded in error messages.
   The message becomes an exception rendered in the UI and logged verbatim
   (Metabase does not truncate downstream), so a multi-MB HTML error page
   must not travel whole."
  1000)

(defn- ^:private truncate-body
  "Truncate an error-response body to [[max-error-body-chars]]."
  [body]
  (let [s (str body)]
    (if (> (count s) max-error-body-chars)
      (str (subs s 0 max-error-body-chars) "… (truncated)")
      s)))

(defn- ^:private status->error-kind
  "Classify a non-200 HTTP status: auth failures and server-side errors are
   endpoint/availability problems (`:db`), anything else is the endpoint
   rejecting the query itself (`:query`, e.g. a 400 SPARQL parse error)."
  [status]
  (if (or (contains? #{401 403 407} status)
          (and (int? status) (>= status 500)))
    :db
    :query))

(defn- ^:private process-response
  "Turn an HTTP `response` into `[true body]` with the decoded JSON, or into
   `[false message kind]`.

   `kind` is `:db` for an endpoint problem (auth failure, 5xx, or a 200 whose
   body is not JSON) and `:query` for any other status, taken as the endpoint
   rejecting the query (e.g. a 400 parse error). The error body is truncated
   to [[max-error-body-chars]]."
  [response]
  (if (= 200 (:status response))
    (if-let [body (parse-json-response response)]
      [true body]
      [false "Invalid JSON response from SPARQL endpoint" :db])
    [false
     (str "SPARQL endpoint returned status: " (:status response)
          "\nBody: " (truncate-body (:body response)))
     (status->error-kind (:status response))]))

(defn execute-sparql-query
  "POST `query` to `endpoint` and return `[true body]` with the decoded JSON,
   or `[false message kind]`.

   `kind` is `:query` or `:db` as in `[[process-response]]`, or `:transport`
   when the request never got an answer (connection refused, DNS, TLS).
   `options` is passed to `[[create-http-options]]`. POST keeps queries of any
   length out of the URL."
  [endpoint query options]
  (try
    (let [start-time (System/currentTimeMillis)
          http-options (create-http-options query options)
          response (http/post endpoint http-options)
          end-time (System/currentTimeMillis)
          execution-time (- end-time start-time)]
      (log/debugf "Endpoint: %s" (uri/redact-userinfo endpoint))
      (log/debugf "Ignore SSL validation: %s" (get options :insecure?))
      (log/debugf "SPARQL query: %s" query)
      (log/debugf "SPARQL query execution return status: %s" (:status response))
      (log/debugf "SPARQL query execution time: %d ms" execution-time)
      (log/debugf "--------------------------------")
      (process-response response))
    (catch Exception e
      (log/errorf "Error executing SPARQL query: %s" (.getMessage e))
      [false (.getMessage e) :transport])))

(defn execute-reducible-query
  "Run the SPARQL in `native-query`'s `[:native :query]` and pass its columns
  and rows to `respond`.

  The endpoint always comes from the database details, never from the query,
  so stored credentials cannot be sent elsewhere. On failure it throws instead
  of returning an empty result: a rejected query (e.g. a SPARQL parse error)
  as `invalid-query`, endpoint and transport problems as `db`."
  [native-query _context respond]
  ;; The query itself is logged at debug level: it holds filter values.
  (log/info "Executing SPARQL query")
  (let [database (driver-api/database (driver-api/metadata-provider))
        details  (:details database)
        ;; Always use the admin-configured endpoint. A native query must not be
        ;; able to override it, otherwise stored auth credentials would be sent
        ;; to an arbitrary, query-controlled endpoint.
        endpoint (:endpoint details)
        sparql-query (get-in native-query [:native :query])
        options {:default-graph (:default-graph details)
                 :insecure?     (:use-insecure details)
                 :auth          (auth/http-options details)}
        [success result kind] (execute-sparql-query endpoint sparql-query options)]
    (if success
      (query-processor/process-query-results result respond)
      ;; No log/error here: the low-level catch already logged transport
      ;; failures, and the QP logs every thrown exception.
      (throw (ex-info (str "Error executing SPARQL query: " result)
                      {:type (if (= kind :query)
                               driver-api/qp.error-type.invalid-query
                               driver-api/qp.error-type.db)})))))
