(ns metabase.driver.sparql.query-processor
  "SPARQL Query Processor for Metabase SPARQL Driver

   This namespace handles SPARQL query processing and result transformation.
   Provides functions to extract metadata and convert results to the format expected by Metabase."
  (:require [metabase.driver.sparql.conversion :as conversion]))

(defn process-query-results
  "Processes the results of a SPARQL query (SELECT or ASK).
   
   Parameters:
     result - SPARQL query result in JSON format
     respond - Function to call with metadata and rows
   
   Returns:
     Result of the call to the respond function. For ASK queries, returns a single boolean column. For SELECT queries, returns columns and rows as usual."
  [result respond]
  (if (contains? result :boolean)
    ;; Handle ASK query result
    (let [metadata {:cols [{:name "boolean"
                            :display_name "boolean"
                            :base_type :type/Boolean}]}
          rows [[(:boolean result)]]]
      (respond metadata rows))
    ;; Handle SELECT query result as before
    (let [;; Extracts variable names from the response header
          vars (get-in result [:head :vars])

          ;; Extracts bindings from the response
          bindings (get-in result [:results :bindings])
          first-row (first bindings)

          ;; Determines column types based on the first row
          col-types (reduce (fn [types var-name]
                              (let [binding (get first-row (keyword var-name))]
                                (assoc types var-name
                                       (if binding
                                         (conversion/sparql-type->base-type (:type binding) (:datatype binding))
                                         :type/Text))))
                            {}
                            vars)

        ;; Creates metadata for the result set
          metadata {:cols (map (fn [var-name]
                                 {:name var-name
                                  :display_name var-name
                                  :base_type (get col-types var-name :type/Text)})
                               vars)}

        ;; Transforms the SPARQL JSON bindings into rows with converted values
          rows (map (fn [binding]
                    ;; Converts each binding into a vector of values in the same order as vars
                      (mapv (fn [var-name]
                              (let [var-binding (get binding (keyword var-name))]
                                (if var-binding
                                  (conversion/convert-value var-binding)
                                  nil)))
                            vars))
                    bindings)]

    ;; Calls the respond function with metadata and rows
      (respond metadata rows))))
