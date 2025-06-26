;; SPARQL Driver for Metabase
;;
;; This driver allows Metabase to connect to SPARQL endpoints to query RDF data.
;; It implements a custom approach (not SQL/JDBC based) using HTTP requests
;; to communicate with SPARQL endpoints. The driver supports secure and insecure
;; connections with optional default graph specification.
(ns metabase.driver.sparql
  (:require [metabase.driver :as driver]
            [metabase.driver.sparql.connection :as connection]
            [metabase.driver.sparql.database :as database]
            [metabase.driver.sparql.execute :as execute]
            [metabase.driver.sparql.parameters]))

;; Register the SPARQL driver in Metabase's driver system
;; No sql or sql-jdbc parent because it's a custom driver using clj-http
(driver/register! :sparql)

;; Implements database-supports? multimethod to define supported features.
(doseq [[feature supported?] {:native-parameters true}]
  (defmethod driver/database-supports? [:sparql feature] [_driver _feature _db] supported?))

;; Implements can-connect? multimethod to test SPARQL endpoint connectivity.
(defmethod driver/can-connect? :sparql
  [_ details]
  (connection/can-connect? (:endpoint details)
                           {:default-graph (:default-graph details)
                            :insecure? (:use-insecure details)}))

;; Implements describe-database multimethod to discover RDF classes as tables.
(defmethod driver/describe-database :sparql
  [_ database]
  (let [endpoint (-> database :details :endpoint)
        options {:insecure? (-> database :details :use-insecure)}]
    (database/describe-database endpoint options)))

;; Implements execute-reducible-query multimethod to execute a SPARQL query and process the results for Metabase.
(defmethod driver/execute-reducible-query :sparql
  [_driver native-query _context respond]
  (execute/execute-reducible-query native-query _context respond))
