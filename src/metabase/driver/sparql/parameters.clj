(ns metabase.driver.sparql.parameters
  (:require
   [clojure.string :as str]
   [metabase.driver :as driver]
   [metabase.driver.common.parameters.values :as params.values]))

(defn substitute-native-parameters
  "Substitutes native parameters in the SPARQL query."
  [_driver {:keys [query] :as inner-query}]
  (let [param->value (params.values/query->params-map inner-query)
        substituted-query (reduce (fn [q [k v]]
                                    (str/replace q (re-pattern (str "\\{\\{" k "\\}\\}"))
                                                 (str v)))
                                  query
                                  param->value)]
    (assoc inner-query :query substituted-query)))
