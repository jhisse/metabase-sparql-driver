(ns metabase.driver.sparql.parameters
  "SPARQL Parameter Substitution for Metabase SPARQL Driver

   This namespace handles parameter substitution in SPARQL queries,
   allowing for dynamic query generation with user-provided values."
  (:require
   [clojure.string :as str]
   [metabase.driver.common.parameters.values :as params.values]
   [metabase.util.log :as log]))

(defn substitute-native-parameters
  "Substitutes native parameters in the SPARQL query.

   Parameters:
     _driver - Driver instance (not used)
     inner-query - Map containing the query and parameter values

   Returns:
     Updated query map with parameters replaced by their values

   Usage:
     Used by the driver to replace {{parameter}} placeholders with actual values"
  [_driver inner-query]
  (let [query (:query inner-query)
        template-tags (:template-tags inner-query)
        parameters (:parameters inner-query)]
    (log/debugf "Query: %s" query)
    (log/debugf "Template tags: %s" template-tags)
    (log/debugf "Parameters: %s" parameters)
    (let [param->value (params.values/query->params-map inner-query)
          substituted-query (reduce (fn [q [k v]]
                                      (str/replace q (re-pattern (str "\\{\\{" k "\\}\\}"))
                                                   (str v)))
                                    query
                                    param->value)]
      (assoc inner-query :query substituted-query))))
