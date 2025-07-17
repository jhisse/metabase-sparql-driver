(ns metabase.driver.sparql.query-processor
  "SPARQL Query Processor for Metabase SPARQL Driver

   This namespace handles SPARQL query processing and result transformation.
   Provides functions to extract metadata and convert results to the format expected by Metabase."
  (:require [metabase.driver.sparql.conversion :as conversion]))

(defn- handle-ask
  "Processes the result of an ASK SPARQL query and calls respond with metadata and rows."
  [result respond]
  (let [metadata {:cols [{:name "boolean"
                          :display_name "boolean"
                          :base_type :type/Boolean}]}
        rows [[(:boolean result)]]]
    (respond metadata rows)))

(defn- handle-select
  "Processes the result of a SELECT SPARQL query and calls respond with metadata and rows."
  [result respond]
  (let [vars (get-in result [:head :vars])
        bindings (get-in result [:results :bindings])
        first-row (first bindings)
        col-types (reduce (fn [types var-name]
                            (let [binding (get first-row (keyword var-name))]
                              (assoc types var-name
                                     (if binding
                                       (conversion/sparql-type->base-type (:type binding) (:datatype binding))
                                       :type/Text))))
                          {}
                          vars)
        metadata {:cols (map (fn [var-name]
                               {:name var-name
                                :display_name var-name
                                :base_type (get col-types var-name :type/Text)})
                             vars)}
        rows (map (fn [binding]
                    (mapv (fn [var-name]
                            (when-let [var-binding (get binding (keyword var-name))]
                              (conversion/convert-value var-binding)))
                          vars))
                  bindings)]
    (respond metadata rows)))

(defn process-query-results
  "Processes the results of a SPARQL query (SELECT or ASK).
   
   Parameters:
     result - SPARQL query result in JSON format
     respond - Function to call with metadata and rows
   
   Returns:
     Result of the call to the respond function. For ASK queries, returns a single boolean column. For SELECT queries, returns columns and rows as usual."
  [result respond]
  (if (contains? result :boolean)
    (handle-ask result respond)
    (handle-select result respond)))
