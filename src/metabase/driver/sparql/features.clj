(ns metabase.driver.sparql.features
  "Decide the driver features that depend on the endpoint. `:now` is probed
   with a live query; the others answer without contacting the endpoint."
  (:require [metabase.util.log :as log]
            [metabase.driver.sparql.auth :as auth]
            [metabase.driver.sparql.execute :as execute]
            [metabase.driver.sparql.templates :as templates]))

;; Define multimethod database-supports? that uses the feature as dispatch value
(defmulti database-supports?
  "True when the SPARQL endpoint of `database` supports `feature`. Dispatches
   on the feature keyword; features without a method are unsupported."
  (fn [_driver feature _database] feature))

;; Default implementation for unsupported features
(defmethod database-supports? :default
  [_driver feature _database]
  (log/debugf "[database-supports?] - No specific implementation for feature: %s, returning false" feature)
  false)

(defmethod database-supports? :now
  [_driver _feature database]
  (log/debugf "[database-supports?] - Checking now() function support")
  (let [details          (:details database)
        endpoint         (:endpoint details)
        options          (execute/probe-options {:default-graph (:default-graph details)
                                                 :insecure?     (:use-insecure details)
                                                 :auth          (auth/http-options details)})
        [success result] (execute/execute-sparql-query endpoint
                                                       (templates/now-function-support-query)
                                                       options)]
    (if success
      (let [bindings (get-in result [:results :bindings])]
        (if (and (seq bindings)
                 (get-in bindings [0 :currentDateTime]))
          (do
            (log/debugf "[database-supports?] - now() function is supported")
            true)
          (do
            (log/debugf "[database-supports?] - now() function is not supported (empty or invalid result)")
            false)))
      (do
        (log/debugf "[database-supports?] - now() function is not supported (query failed: %s)" result)
        false))))

;; Basic aggregations (count, count-distinct, sum, avg, min, max) are compiled to
;; SPARQL aggregate functions with an optional GROUP BY. SPARQL 1.1 endpoints all
;; support these, so we report the feature as available unconditionally.
(defmethod database-supports? :basic-aggregations
  [_driver _feature _database]
  true)