(ns metabase.driver.sparql.connection
  "Test that a SPARQL endpoint is reachable and detect the SPARQL version it
   supports."
  (:require [metabase.util.log :as log]
            [metabase.driver.sparql.auth :as auth]
            [metabase.driver.sparql.execute :as execute]
            [metabase.driver.sparql.templates :as templates]
            [metabase.driver.sparql.uri :as uri]))

(defn can-connect?
  "Return true when `endpoint` answers the connection test query, or throw
   \"Connection failed: …\" with the endpoint's error.

   `options` takes `:default-graph`, `:insecure?` and `:auth` (from
   `[[auth/http-options]]`). The request uses the probe timeout of
   [[execute/probe-options]]."
  [endpoint options]
  (log/info "Trying to connect to SPARQL endpoint:" (uri/redact-userinfo endpoint))
  (let [[success result] (execute/execute-sparql-query endpoint
                                                       (templates/connection-test-query)
                                                       (execute/probe-options options))]
    (if success
      true
      (throw (Exception. (str "Connection failed: " result))))))

(defn dbms-version
  "Return the SPARQL version the endpoint of `database` supports, as
   `{:version \"SPARQL 1.1\"}`.

   Both a BIND probe and a VALUES probe are run (both are SPARQL 1.1); when
   neither succeeds, the errors are logged and the result is
   `{:version \"SPARQL 1.0\"}`."
  [_driver database]
  (let [details          (:details database)
        endpoint         (:endpoint details)
        options          (execute/probe-options {:default-graph (:default-graph details)
                                                 :insecure?     (:use-insecure details)
                                                 :auth          (auth/http-options details)})
        [ok-bind res-bind]     (execute/execute-sparql-query endpoint (templates/sparql-1-1-bind-version-query) options)
        [ok-values res-values] (execute/execute-sparql-query endpoint (templates/sparql-1-1-values-version-query) options)
        result  (cond
                  ok-bind   res-bind
                  ok-values res-values
                  :else     nil)]
    (if result
      (let [bindings (get-in result [:results :bindings])]
        (log/debugf "SPARQL endpoint supports 1.1 features.")
        {:version (get-in bindings [0 :version :value])})
      (do
        (log/errorf "Error getting SPARQL version: [Bind] %s" res-bind)
        (log/errorf "Error getting SPARQL version: [Values] %s" res-values)
        {:version "SPARQL 1.0"})))) ;; Default to SPARQL 1.0 if no version is detected (or would "unknown" be better?)