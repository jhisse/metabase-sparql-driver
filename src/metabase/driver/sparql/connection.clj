(ns metabase.driver.sparql.connection
  "SPARQL Connection for Metabase SPARQL Driver

   This namespace manages connections to SPARQL endpoints.
   Provides functions to test connectivity and manage connection details."
  (:require [metabase.util.log :as log]
            [metabase.driver.sparql.auth :as auth]
            [metabase.driver.sparql.execute :as execute]
            [metabase.driver.sparql.templates :as templates]))

(defn can-connect?
  "Return true when `endpoint` answers the connection test query, or throw
   \"Connection failed: …\" with the endpoint's error.

   `options` takes `:default-graph`, `:insecure?` and `:auth` (from
   `[[auth/http-options]]`)."
  [endpoint options]
  (log/info "Trying to connect to SPARQL endpoint:" endpoint)
  (let [[success result] (execute/execute-sparql-query endpoint (templates/connection-test-query) options)]
    (if success
      true
      (throw (Exception. (str "Connection failed: " result))))))

(defn dbms-version
  "Return the SPARQL version the endpoint supports, as `{:version \"SPARQL 1.1\"}`.

   1.1 is detected by running a BIND query and a VALUES query; when neither
   answers, the version falls back to `{:version \"SPARQL 1.0\"}`."
  [_driver database]
  (let [details          (:details database)
        endpoint         (:endpoint details)
        options          {:default-graph (:default-graph details)
                          :insecure?     (:use-insecure details)
                          :auth          (auth/http-options details)}
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