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

;; Implements humanize-connection-error-message multimethod to provide user-friendly error messages.
(defmethod driver/humanize-connection-error-message :sparql
  [_ message]
  ;; TODO: add humanized error messages for SPARQL driver.
  message)

;; Implements dbms-version multimethod to define the version of the SPARQL endpoint.
(defmethod driver/dbms-version :sparql
  [_ _]
  ;; TODO: add dbms-version for SPARQL driver.
  ;; This methos can discovery the version of the SPARQL endpoint by sending a SPARQL query to the endpoint.
  ;; Check service description in https://www.w3.org/TR/2013/REC-sparql11-service-description-20130321/ 
  ;; Sparql 1.0 vs 1.1 can be detected by checking the service description.
  ;; Some features can be only available in SPARQL 1.1
  nil)

;; Implements database-supports? multimethod to define supported features.
(doseq [[feature supported?] {:metadata/key-constraints false
                              :nested-fields false
                              :nested-field-columns false
                              :set-timezone false
                              :basic-aggregations true ;; Only in SPARQL 1.1
                              :standard-deviation-aggregations false
                              :expressions true
                              :native-parameters    true
                              :expression-aggregations true ;; Only in SPARQL 1.1
                              :expression-literals true
                              :nested-queries true ;; Only in SPARQL 1.1
                              :native-parameter-card-reference false ;; Can be possible in the future if careful implementation
                              :persist-models false
                              :persist-models-enabled false
                              :binning false
                              :case-sensitivity-string-filter-options true
                              :left-join true
                              :right-join false
                              :inner-join true
                              :full-join false
                              :regex true
                              :advanced-math-expressions false
                              :percentile-aggregations false
                              :temporal-extract true ;; Only in SPARQL 1.1
                              :date-arithmetics true ;; Only in SPARQL 1.1
                              :now true ;; Only in SPARQL 1.1
                              :convert-timezone false
                              :datetime-diff false
                              :actions false ;; Will not support writeback actions by now
                              :table-privileges false
                              :uploads false
                              :schemas true ;; SPARQL does not have schemas but we can use it to group RDF graphs
                              :multi-level-schema false
                              :actions/custom false ;; Will not support writeback actions by now
                              :test/jvm-timezone-setting false
                              :connection-impersonation-requires-role false
                              :native-requires-specified-collection false
                              :index-info false
                              :describe-fks false
                              :describe-fields false ;; Can be slow in big datasets
                              :describe-indexes false
                              :upload-with-auto-pk false
                              :fingerprint false
                              :connection/multiple-databases false
                              :identifiers-with-spaces false
                              :uuid-type false
                              :split-part false
                              :temporal/requires-default-unit false
                              :window-functions/cumulative false
                              :window-functions/offset false
                              :parameterized-sql false
                              :distinct-where false
                              :saved-question-sandboxing false
                              :expressions/integer true
                              :expressions/text false
                              :expressions/date false
                              :expressions/datetime false
                              :expressions/float false
                              :test/dynamic-dataset-loading false
                              :test/creates-db-on-connect false
                              :test/cannot-destroy-db true
                              :test/uuids-in-create-table-statements false
                              :database-routing false}]
  (defmethod driver/database-supports? [:sparql feature] [_driver _feature _db] supported?))

;; Implements database-supports? multimethod to check if a feature is supported by SPARQL endpoint version.
(doseq [[feature supported?] {:basic-aggregations true
                              :expression-aggregations true
                              :nested-queries true
                              :temporal-extract true
                              :date-arithmetics true
                              :now true}]
  ;; TODO: implement when driver/dbms-version is implemented
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
