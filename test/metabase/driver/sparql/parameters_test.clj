(ns metabase.driver.sparql.parameters-test
  "Unit tests for SPARQL parameter substitution"
  (:require [clojure.test :refer :all]
            [metabase.driver.common.parameters :as params]
            [metabase.driver.sparql.parameters :as parameters]
            [metabase.driver.sparql.test-util :as tu]))

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

(deftest iri-parameter-with-illegal-chars-is-percent-encoded
  (testing "A scheme-shaped value with IRIREF-illegal chars is percent-encoded, not leaked raw"
    (is (= "SELECT * WHERE { ?s a <https://example.org/a%3E%20b> }"
           (subst {:query         "SELECT * WHERE { ?s a {{cls}} }"
                   :template-tags {"cls" {:name "cls" :display-name "Class" :type :text}}
                   :parameters    [{:type "category"
                                    :target [:variable [:template-tag "cls"]]
                                    :value "https://example.org/a> b"}]})))))

(deftest whitespace-inside-braces-is-tolerated
  (testing "`{{ name }}` and `{{name}}` are both substituted"
    (is (= "SELECT * WHERE { ?s rdfs:label \"Alice\" }"
           (subst {:query         "SELECT * WHERE { ?s rdfs:label {{ name }} }"
                   :template-tags {"name" {:name "name" :display-name "Name" :type :text}}
                   :parameters    [{:type "category"
                                    :target [:variable [:template-tag "name"]]
                                    :value "Alice"}]})))))

(deftest valueless-tag-stays-as-written
  (let [tags {"x" {:name "x" :display-name "X" :type :text}}]
    (testing "a tag in a `#` comment does not break the query"
      (let [q "SELECT ?s WHERE { ?s ?p ?o\n# FILTER(?s = {{x}})\n}"]
        (is (= q (subst {:query q :template-tags tags :parameters []})))))
    (testing "a nested group written `{{ … }}` is not a tag"
      (let [q "SELECT ?s WHERE {{ ?s a <https://example.org/A> } UNION { ?s a <https://example.org/B> }}"
            out (subst {:query q :template-tags tags :parameters []})]
        (is (= "SELECT ?s WHERE {{?s a <https://example.org/A> } UNION { ?s a <https://example.org/B>}}" out)
            "the parser trims the inner edges, which SPARQL ignores")
        (is (nil? (tu/sparql-syntax-error out)) out)))))

(deftest optional-clause
  (let [q    "SELECT * WHERE { ?s rdfs:label ?l [[FILTER(?l = {{name}})]] }"
        tags {"name" {:name "name" :display-name "Name" :type :text}}]
    (testing "dropped when its parameter has no value"
      (is (= "SELECT * WHERE { ?s rdfs:label ?l  }"
             (subst {:query q :template-tags tags :parameters []}))))
    (testing "kept, brackets removed, when it has a value"
      (is (= "SELECT * WHERE { ?s rdfs:label ?l FILTER(?l = \"Alice\") }"
             (subst {:query q :template-tags tags
                     :parameters [{:type "category" :target [:variable [:template-tag "name"]] :value "Alice"}]}))))))

(deftest multi-value-renders-as-comma-list
  (testing "A vector of values renders as a comma-separated SPARQL term list"
    (is (= "SELECT * WHERE { ?s a ?c FILTER (?c IN (<https://example.org/A>, <https://example.org/B>)) }"
           (subst {:query         "SELECT * WHERE { ?s a ?c FILTER (?c IN ({{cls}})) }"
                   :template-tags {"cls" {:name "cls" :display-name "Class" :type :text}}
                   :parameters    [{:type "category"
                                    :target [:variable [:template-tag "cls"]]
                                    :value ["https://example.org/A" "https://example.org/B"]}]})))))

(deftest multi-value-percent-encodes-illegal-chars-per-element
  (testing "Each IRI element of an IN(...) list is individually percent-encoded"
    (is (= "SELECT * WHERE { ?s a ?c FILTER (?c IN (<https://example.org/a%3Eb>, <https://example.org/c%20d>)) }"
           (subst {:query         "SELECT * WHERE { ?s a ?c FILTER (?c IN ({{cls}})) }"
                   :template-tags {"cls" {:name "cls" :display-name "Class" :type :text}}
                   :parameters    [{:type "category"
                                    :target [:variable [:template-tag "cls"]]
                                    :value ["https://example.org/a>b" "https://example.org/c d"]}]})))))

(deftest value-with-regex-meta-chars-survives
  (testing "A `$` or `\\` in the value is not interpreted as a regex backreference"
    (is (= "SELECT * WHERE { ?s rdfs:label \"$1 backslash\\\\here\" }"
           (subst {:query         "SELECT * WHERE { ?s rdfs:label {{x}} }"
                   :template-tags {"x" {:name "x" :display-name "X" :type :text}}
                   :parameters    [{:type "category"
                                    :target [:variable [:template-tag "x"]]
                                    :value "$1 backslash\\here"}]})))))

(deftest date-parameter-is-typed
  (letfn [(date [value]
            (subst {:query         "SELECT * WHERE { ?s ex:d ?d FILTER(?d > {{d}}) }"
                    :template-tags {"d" {:name "d" :display-name "D" :type :date}}
                    :parameters    [{:type   "date/single"
                                     :target [:variable [:template-tag "d"]]
                                     :value  value}]}))]
    (testing "a date compares as xsd:date, not as a plain string"
      (is (= "SELECT * WHERE { ?s ex:d ?d FILTER(?d > \"2005-01-01\"^^<http://www.w3.org/2001/XMLSchema#date>) }"
             (date "2005-01-01"))))
    (testing "a date with a time is an xsd:dateTime, seconds added"
      (is (= "SELECT * WHERE { ?s ex:d ?d FILTER(?d > \"2005-01-01T10:30:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>) }"
             (date "2005-01-01T10:30"))))))

(deftest field-filter-is-a-clear-error
  (testing "a Field Filter fails with a clear message instead of a generic endpoint 400"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"does not support Field Filter variables"
                          (#'parameters/->sparql-term (params/map->FieldFilter {:field {} :value "x"}))))))
