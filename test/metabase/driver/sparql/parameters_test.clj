(ns metabase.driver.sparql.parameters-test
  "Unit tests for SPARQL parameter substitution"
  (:require [clojure.test :refer :all]
            [metabase.driver.sparql.parameters :as parameters]))

(defn- subst [inner-query]
  (:query (parameters/substitute-native-parameters :sparql inner-query)))

(deftest no-parameters
  (testing "Query without any template tags is returned unchanged"
    (is (= "SELECT * WHERE { ?s rdfs:label ?name }"
           (subst {:query "SELECT * WHERE { ?s rdfs:label ?name }"})))))

(deftest text-parameter-is-quoted
  (testing "A raw text value is rendered as a SPARQL string literal"
    (is (= "SELECT * WHERE { ?s rdfs:label \"Alice\" }"
           (subst {:query         "SELECT * WHERE { ?s rdfs:label {{name}} }"
                   :template-tags {"name" {:name "name" :display-name "Name" :type :text}}
                   :parameters    [{:type "category"
                                    :target [:variable [:template-tag "name"]]
                                    :value "Alice"}]})))))

(deftest text-parameter-escapes-quotes-and-backslashes
  (testing "Embedded quotes and backslashes survive escaping"
    (is (= "SELECT * WHERE { ?s rdfs:label \"a\\\\b\\\"c\" }"
           (subst {:query         "SELECT * WHERE { ?s rdfs:label {{x}} }"
                   :template-tags {"x" {:name "x" :display-name "X" :type :text}}
                   :parameters    [{:type "category"
                                    :target [:variable [:template-tag "x"]]
                                    :value "a\\b\"c"}]})))))

(deftest numeric-parameter-is-bare
  (testing "A number parameter is rendered as a bare literal"
    (is (= "SELECT * WHERE { ?s ex:age 25 }"
           (subst {:query         "SELECT * WHERE { ?s ex:age {{age}} }"
                   :template-tags {"age" {:name "age" :display-name "Age" :type :number}}
                   :parameters    [{:type "number/="
                                    :target [:variable [:template-tag "age"]]
                                    :value 25}]})))))

(deftest iri-parameter-is-bracketed
  (testing "A URL-shaped value is wrapped in angle brackets as a SPARQL IRI"
    (is (= "SELECT * WHERE { ?s a <https://data.example.org/Item> }"
           (subst {:query         "SELECT * WHERE { ?s a {{cls}} }"
                   :template-tags {"cls" {:name "cls" :display-name "Class" :type :text}}
                   :parameters    [{:type "category"
                                    :target [:variable [:template-tag "cls"]]
                                    :value "https://data.example.org/Item"}]})))))

(deftest whitespace-inside-braces-is-tolerated
  (testing "`{{ name }}` and `{{name}}` are both substituted"
    (is (= "SELECT * WHERE { ?s rdfs:label \"Alice\" }"
           (subst {:query         "SELECT * WHERE { ?s rdfs:label {{ name }} }"
                   :template-tags {"name" {:name "name" :display-name "Name" :type :text}}
                   :parameters    [{:type "category"
                                    :target [:variable [:template-tag "name"]]
                                    :value "Alice"}]})))))

(deftest missing-optional-leaves-placeholder
  (testing "An optional tag with no value leaves the `{{x}}` literal in place"
    (is (= "SELECT * WHERE { ?s rdfs:label {{name}} }"
           (subst {:query         "SELECT * WHERE { ?s rdfs:label {{name}} }"
                   :template-tags {"name" {:name "name" :display-name "Name" :type :text}}
                   :parameters    []})))))

(deftest multi-value-renders-as-comma-list
  (testing "A vector of values renders as a comma-separated SPARQL term list"
    (is (= "SELECT * WHERE { ?s a ?c FILTER (?c IN (<https://ex.org/A>, <https://ex.org/B>)) }"
           (subst {:query         "SELECT * WHERE { ?s a ?c FILTER (?c IN ({{cls}})) }"
                   :template-tags {"cls" {:name "cls" :display-name "Class" :type :text}}
                   :parameters    [{:type "category"
                                    :target [:variable [:template-tag "cls"]]
                                    :value ["https://ex.org/A" "https://ex.org/B"]}]})))))

(deftest value-with-regex-meta-chars-survives
  (testing "A `$` or `\\` in the value is not interpreted as a regex backreference"
    (is (= "SELECT * WHERE { ?s rdfs:label \"$1 backslash\\\\here\" }"
           (subst {:query         "SELECT * WHERE { ?s rdfs:label {{x}} }"
                   :template-tags {"x" {:name "x" :display-name "X" :type :text}}
                   :parameters    [{:type "category"
                                    :target [:variable [:template-tag "x"]]
                                    :value "$1 backslash\\here"}]})))))
