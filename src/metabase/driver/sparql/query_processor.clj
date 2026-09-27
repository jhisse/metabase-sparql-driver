(ns metabase.driver.sparql.query-processor
  "SPARQL Query Processor for Metabase SPARQL Driver

   This namespace handles SPARQL query processing and result transformation.
   Provides functions to extract metadata and convert results to the format expected by Metabase."
  (:require [metabase.driver-api.core :as driver-api]
            [metabase.driver.sparql.conversion :as conversion]))

(defn- handle-ask
  "Pass an ASK `result` to `respond` as one row with one boolean column."
  [result respond]
  (let [metadata {:cols [{:name "boolean"
                          :display_name "boolean"
                          :base_type :type/Boolean}]}
        rows [[(:boolean result)]]]
    (respond metadata rows)))

(defn- handle-select
  "Pass a SELECT `result` to `respond`, one column per variable, with each
  column's type inferred from its values."
  [result respond]
  (let [vars (get-in result [:head :vars])
        bindings (get-in result [:results :bindings])
        col-types (conversion/determine-column-types vars bindings)
        ;; No :display_name: Metabase lets the driver's column keys override Lib's,
        ;; so sending the SPARQL var would label MBQL columns `ag_0`. Native
        ;; queries fall back to :name.
        metadata {:cols (map (fn [var-name]
                               {:name var-name
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
  "Pass a SELECT or ASK `result` to `respond` as Metabase columns and rows, and
   return what `respond` returns.

   Any other result (a CONSTRUCT or DESCRIBE graph) throws `invalid-query`."
  [result respond]
  (cond
    (and (map? result) (contains? result :boolean)) (handle-ask result respond)
    (and (map? result) (contains? result :head))    (handle-select result respond)
    ;; CONSTRUCT / DESCRIBE answer with a graph (e.g. a JSON-LD array), not a table.
    :else (throw (ex-info "Only SELECT and ASK queries are supported; CONSTRUCT and DESCRIBE return graphs."
                          {:type driver-api/qp.error-type.invalid-query}))))
