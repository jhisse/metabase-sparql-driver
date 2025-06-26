;; SPARQL Driver for Metabase
;;
;; This driver enables Metabase to connect to SPARQL endpoints for querying RDF data.
;; It implements a custom approach (not based on SQL/JDBC) using HTTP requests
;; to communicate with SPARQL endpoints. The driver supports both secure and insecure
;; connections with optional default graph specification.
(ns metabase.driver.sparql
  (:require [clojure.tools.logging :as log]
            [metabase.driver :as driver]
            [metabase.query-processor.store :as qp.store]
            [metabase.lib.metadata :as lib.metadata]
            [clojure.string :as str]
            [metabase.driver.sparql.sparql-templates :as sparql-templates]
            [metabase.driver.sparql.execute :as execute]))

;; Register SPARQL driver with Metabase's driver system
;; No parent sql or sql-jdbc because it is a custom driver using clj-http
;; This registration makes the driver available to Metabase for database connections
(driver/register! :sparql)

;; Method to test connection to SPARQL endpoint
;; This method verifies if Metabase can successfully connect to the specified SPARQL endpoint
;;
;; Parameters:
;;   _ - Driver instance (ignored)
;;   details - A map containing connection details with the following keys:
;;     :endpoint - URL of the SPARQL endpoint (required)
;;     :default-graph - URI of the default graph to query (optional)
;;     :insecure - Boolean flag to ignore SSL certificate validation (optional)
;;
;; Returns:
;;   Boolean - true if connection successful (HTTP 200 response), false otherwise
;;
;; Behavior:
;;   1. Executes a simple SPARQL query to test connectivity
;;   2. Logs warnings if using insecure connection
;;   3. Returns false and logs error on any exception
(defmethod driver/can-connect? :sparql
  [_ details]
  (log/info "Attempting to connect to SPARQL endpoint:" (:endpoint details))

  (let [endpoint (:endpoint details)
        options {:default-graph (:default-graph details)
                 :insecure? (:insecure details)}
        [success _] (execute/execute-sparql-query endpoint (sparql-templates/connection-test-query) options)]
    success))

;; Implementation of describe-database method for SPARQL driver
;; This method discovers the available "tables" (RDF classes) in the SPARQL endpoint
;;
;; Parameters:
;;   _ - Driver instance (ignored)
;;   database - Database instance containing connection details
;;
;; Returns:
;;   A map with :tables key containing a set of table definitions, where each table
;;   represents an RDF class found in the endpoint
;;
;; Behavior:
;;   1. Queries the endpoint for distinct RDF classes (limited to 100)
;;   2. Extracts the class URIs from the response
;;   3. Converts each class URI to a table definition by extracting the class name
;;      from the last part of the URI (after the last '/' or '#')
;;   4. Returns empty table set and logs error on any exception
(defmethod driver/describe-database :sparql
  [_ database]
  (let [endpoint (-> database :details :endpoint)
        ;; SPARQL query to get all distinct classes in the endpoint
        options {:insecure? (-> database :details :insecure)}
        [success result] (execute/execute-sparql-query endpoint (sparql-templates/classes-discovery-query) options)]

    (if success
      (let [;; Extract class URIs and instance counts from the SPARQL JSON response
            ;; Each binding contains :class and :count keys with their respective values
            classes-with-counts (map (fn [binding]
                                       {:uri (get-in binding [:class :value])
                                        :count (Integer/parseInt (get-in binding [:count :value]))})
                                     (get-in result [:results :bindings]))]
        {:tables
         (set
          (for [{:keys [uri count]} classes-with-counts
                :let [class-name (last (str/split uri #"[/#]"))]]
            {:name         class-name
             :schema       nil
             :display_name class-name
             :description  (str "Class with " count " instances")}))})
      (do
        (log/error "Error describing SPARQL database:" result)
        {:tables #{}}))))

;; Implementation of execute-reducible-query method for SPARQL driver
;; This method executes a SPARQL query and returns the results in a reducible format
;;
;; Parameters:
;;   _driver - Driver instance (ignored)
;;   query - Query map containing the native SPARQL query
;;   _context - Query context (ignored)
;;   respond - Function to call with results metadata and rows
;;
;; Returns:
;;   The result of calling the respond function with metadata and rows
;;
;; Behavior:
;;   1. Extracts the SPARQL query and endpoint from the input
;;   2. Executes the query against the SPARQL endpoint
;;   3. Extracts column metadata from the response headers
;;   4. Transforms the response bindings to rows
;;   5. Calls the respond function with the metadata and rows
(defmethod driver/execute-reducible-query :sparql
  [_driver {{sparql-query :query endpoint :endpoint} :native, :as native-query} _context respond]
  (log/info "Executing SPARQL query:" (pr-str (select-keys native-query [:native])))

  (let [database (lib.metadata/database (qp.store/metadata-provider))
        endpoint (or endpoint (-> database :details :endpoint))
        options {:default-graph (-> database :details :default-graph)
                 :insecure? (-> database :details :insecure)}

        ;; Execute the SPARQL query
        [success result] (execute/execute-sparql-query endpoint sparql-query options)]

    (if success
      (let [;; Extract variable names from the response header
            vars (get-in result [:head :vars])

            ;; Define mappings for SPARQL types to Metabase base types
            sparql-type->base-type (fn [sparql-type datatype]
                                     (cond
                                       ;; URIs are represented as text
                                       (= sparql-type "uri") :type/URL

                                       ;; Handle blank nodes
                                       (= sparql-type "bnode") :type/Text

                                       ;; Handle typed literals (common in DBpedia and other endpoints)
                                       (and (= sparql-type "typed-literal") datatype)
                                       (cond
                                         (str/includes? datatype "integer") :type/Integer
                                         (str/includes? datatype "decimal") :type/Float
                                         (str/includes? datatype "float") :type/Float
                                         (str/includes? datatype "double") :type/Float
                                         (str/includes? datatype "boolean") :type/Boolean
                                         (str/includes? datatype "date") :type/Date
                                         (str/includes? datatype "dateTime") :type/DateTime
                                         (str/includes? datatype "time") :type/Time
                                         :else :type/Text)

                                       ;; Handle regular literals with explicit datatypes
                                       (and (= sparql-type "literal") datatype)
                                       (cond
                                         (str/includes? datatype "integer") :type/Integer
                                         (str/includes? datatype "decimal") :type/Float
                                         (str/includes? datatype "float") :type/Float
                                         (str/includes? datatype "double") :type/Float
                                         (str/includes? datatype "boolean") :type/Boolean
                                         (str/includes? datatype "date") :type/Date
                                         (str/includes? datatype "dateTime") :type/DateTime
                                         (str/includes? datatype "time") :type/Time
                                         (str/includes? datatype "XMLLiteral") :type/Text
                                         :else :type/Text)

                                       ;; Literals with language tags are text
                                       :else :type/Text))

            ;; Extract column types and convert values based on the first row of results
            bindings (get-in result [:results :bindings])
            first-row (first bindings)

            ;; Determine column types from the first row or default to Text if no data
            col-types (if first-row
                        (reduce (fn [types var-name]
                                  (let [binding (get first-row (keyword var-name))
                                        sparql-type (:type binding)
                                        datatype (:datatype binding)
                                        base-type (sparql-type->base-type sparql-type datatype)]
                                    (assoc types var-name base-type)))
                                {}
                                vars)
                        (reduce #(assoc %1 %2 :type/Text) {} vars))

            ;; Create column metadata with appropriate types
            metadata {:cols (mapv (fn [var-name]
                                    {:name var-name
                                     :display_name (str/capitalize var-name)
                                     :base_type (get col-types var-name :type/Text)})
                                  vars)}

            ;; Function to convert value based on SPARQL type and datatype
            convert-value (fn [binding]
                            (let [value (:value binding)
                                  type-key (:type binding)
                                  datatype (:datatype binding)]
                              (cond
                               ;; Handle typed-literal integers (common in DBpedia)
                                (and (= type-key "typed-literal")
                                     datatype
                                     (str/includes? datatype "integer"))
                                (try (Long/parseLong value)
                                     (catch Exception e
                                       (log/warn "Failed to parse integer:" value "Error:" (.getMessage e))
                                       value))

                               ;; Handle typed-literal decimals/floats
                                (and (= type-key "typed-literal")
                                     datatype
                                     (or (str/includes? datatype "decimal")
                                         (str/includes? datatype "float")
                                         (str/includes? datatype "double")))
                                (try (Double/parseDouble value)
                                     (catch Exception e
                                       (log/warn "Failed to parse float:" value "Error:" (.getMessage e))
                                       value))

                               ;; Handle typed-literal booleans
                                (and (= type-key "typed-literal")
                                     datatype
                                     (str/includes? datatype "boolean"))
                                (Boolean/parseBoolean value)

                               ;; Handle regular literal integers
                                (and (= type-key "literal")
                                     datatype
                                     (str/includes? datatype "integer"))
                                (try (Long/parseLong value)
                                     (catch Exception e
                                       (log/warn "Failed to parse integer:" value "Error:" (.getMessage e))
                                       value))

                               ;; Handle regular literal decimals/floats
                                (and (= type-key "literal")
                                     datatype
                                     (or (str/includes? datatype "decimal")
                                         (str/includes? datatype "float")
                                         (str/includes? datatype "double")))
                                (try (Double/parseDouble value)
                                     (catch Exception e
                                       (log/warn "Failed to parse float:" value "Error:" (.getMessage e))
                                       value))

                               ;; Handle regular literal booleans
                                (and (= type-key "literal")
                                     datatype
                                     (str/includes? datatype "boolean"))
                                (Boolean/parseBoolean value)

                               ;; Default: return the string value
                                :else value)))

            ;; Transform the SPARQL JSON response bindings to rows with converted values
            rows (map (fn [binding]
                        ;; Convert each binding to a vector of values in the same order as vars
                        (mapv (fn [var-name]
                                (let [var-binding (get binding (keyword var-name))]
                                  (if var-binding
                                    (convert-value var-binding)
                                    nil)))
                              vars))
                      bindings)]

        ;; Call the respond function with metadata and rows
        (respond metadata rows))

      ;; If query execution failed, log the error and return empty results
      (do
        (log/error "Error executing SPARQL query:" result)
        (respond {:cols []} [])))))
