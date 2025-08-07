(ns metabase.driver.sparql.parameters-test
  "Unit tests for SPARQL parameter substitution"
  (:require [clojure.test :refer :all]
            [metabase.driver.sparql.parameters :as parameters]))

(deftest test-substitute-native-parameters
  (testing "Handle query without parameters"
    (let [inner-query {:query "SELECT * WHERE { ?s rdfs:label ?name }"}
          result (parameters/substitute-native-parameters :sparql inner-query)]
      (is (= "SELECT * WHERE { ?s rdfs:label ?name }" (:query result)))))

  (testing "Handle single parameter substitution"
    (let [inner-query {:query "SELECT * WHERE { ?s rdfs:label {{name}} }"
                       :template-tags {"name" {:name "name"
                                               :display-name "Name"
                                               :type "text"}}
                       :parameters [{:type "text"
                                     :target [:variable [:template-tag "name"]]
                                     :value "\"Alice\""}]}
          result (parameters/substitute-native-parameters :sparql inner-query)]
      (is (= "SELECT * WHERE { ?s rdfs:label \"Alice\" }" (:query result)))))

  (testing "Handle numeric parameter substitution"
    (let [inner-query {:query "SELECT * WHERE { ?s ex:age {{age}} }"
                       :template-tags {"age" {:name "age"
                                              :display-name "Age"
                                              :type "number"}}
                       :parameters [{:type "number"
                                     :target [:variable [:template-tag "age"]]
                                     :value 25}]}
          result (parameters/substitute-native-parameters :sparql inner-query)]
      (is (= "SELECT * WHERE { ?s ex:age 25 }" (:query result))))))