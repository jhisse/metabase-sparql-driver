(ns metabase.driver.sparql.features
  "SPARQL Features Detection for Metabase SPARQL Driver

   This namespace provides functions to detect supported features in SPARQL endpoints.
   It implements tests for specific SPARQL features by executing test queries and analyzing results."
  (:require [metabase.util.log :as log]
            [metabase.driver.sparql.execute :as execute]
            [metabase.driver.sparql.templates :as templates]))

;; Define multimethod database-supports? that uses the feature as dispatch value
(defmulti database-supports?
  "Checks if a specific feature is supported by the SPARQL endpoint.
   
   Dispatches on the feature keyword to determine the appropriate implementation."
  (fn [_driver feature _database] feature))

;; Default implementation for unsupported features
(defmethod database-supports? :default
  [_driver feature _database]
  (log/debugf "[database-supports?] - No specific implementation for feature: %s, returning false" feature)
  false)

(defmethod database-supports? :now
  [_driver _feature database]
  (log/debugf "[database-supports?] - Checking now() function support")
  (let [endpoint         (-> database :details :endpoint)
        options          {:default-graph (-> database :details :default-graph)
                          :insecure?    (-> database :details :use-insecure)}
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

;; Example of how to add support for another feature in the future:
;; 
;; (defmethod database-supports? :basic-aggregation
;;   [_driver _feature database]
;;   (log/debugf "[database-supports?] - Checking COUNT aggregation support")
;;   ;; Specific implementation to verify support for COUNT aggregation
;;   )