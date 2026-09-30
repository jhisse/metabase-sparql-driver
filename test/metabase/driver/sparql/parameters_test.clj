(ns metabase.driver.sparql.parameters-test
  "Unit tests for SPARQL parameter substitution"
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
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
  (let [label "<http://www.w3.org/2000/01/rdf-schema#label>"
        q     (str "SELECT * WHERE { ?s " label " ?l [[FILTER(?l = {{name}})]] }")
        tags  {"name" {:name "name" :display-name "Name" :type :text}}]
    (testing "dropped when its parameter has no value"
      (let [out (subst {:query q :template-tags tags :parameters []})]
        (is (= (str "SELECT * WHERE { ?s " label " ?l  }") out))
        (is (nil? (tu/sparql-syntax-error out)) out)))
    (testing "kept, brackets removed, when it has a value"
      (let [out (subst {:query q :template-tags tags
                        :parameters [{:type "category" :target [:variable [:template-tag "name"]] :value "Alice"}]})]
        (is (= (str "SELECT * WHERE { ?s " label " ?l FILTER(?l = \"Alice\") }") out))
        (is (nil? (tu/sparql-syntax-error out)) out)))))

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
            (subst {:query         "SELECT * WHERE { ?s <https://example.org/d> ?d FILTER(?d > {{d}}) }"
                    :template-tags {"d" {:name "d" :display-name "D" :type :date}}
                    :parameters    [{:type   "date/single"
                                     :target [:variable [:template-tag "d"]]
                                     :value  value}]}))]
    (testing "a date compares as xsd:date, not as a plain string"
      (let [out (date "2005-01-01")]
        (is (= "SELECT * WHERE { ?s <https://example.org/d> ?d FILTER(?d > \"2005-01-01\"^^<http://www.w3.org/2001/XMLSchema#date>) }" out))
        (is (nil? (tu/sparql-syntax-error out)) out)))
    (testing "a date with a time is an xsd:dateTime, seconds added"
      (let [out (date "2005-01-01T10:30")]
        (is (= "SELECT * WHERE { ?s <https://example.org/d> ?d FILTER(?d > \"2005-01-01T10:30:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>) }" out))
        (is (nil? (tu/sparql-syntax-error out)) out)))
    (testing "seconds go before a timezone, not after it"
      (is (str/includes? (date "2005-01-01T10:30Z") "\"2005-01-01T10:30:00Z\"^^"))
      (is (str/includes? (date "2005-01-01T10:30+02:00") "\"2005-01-01T10:30:00+02:00\"^^"))
      (is (str/includes? (date "2005-01-01T10:30:15Z") "\"2005-01-01T10:30:15Z\"^^")))))

(deftest field-filter-is-a-clear-error
  (testing "a Field Filter fails with a clear message instead of a generic endpoint 400"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"does not support Field Filter variables"
                          (#'parameters/->sparql-term (params/map->FieldFilter {:field {} :value "x"}))))))

(deftest tag-inside-quotes-is-an-error
  (letfn [(subst-tags [q values]
            (subst {:query         q
                    :template-tags (into {} (for [k (keys values)] [k {:name k :display-name k :type :text}]))
                    :parameters    (for [[k v] values :when (some? v)]
                                     {:type "category" :target [:variable [:template-tag k]] :value v})}))]
    (testing "a value that would close the quotes around the tag is refused"
      (doseq [q ["SELECT * WHERE { ?s ?p ?o FILTER(?o = '{{x}}') }"
                 "SELECT * WHERE { ?s ?p ?o FILTER(?o = \"{{x}}\") }"
                 "SELECT * WHERE { ?s ?p ?o FILTER(?o = \"\"\"{{x}}\"\"\") }"
                 "SELECT * WHERE { ?s ?p ?o [[FILTER(?o = '{{x}}')]] }"]]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"\{\{x\}\} variable is written inside quotes"
                              (subst-tags q {"x" "' || true || '"}))
            q)))
    (testing "a quoted tag without a value stays as written"
      (let [q "SELECT * WHERE { ?s ?p '{{x}}' }"]
        (is (= q (subst-tags q {"x" nil})))
        (is (nil? (tu/sparql-syntax-error q)) q)))
    (testing "a number cannot close the quotes, so it is still substituted"
      (let [out (subst {:query         "SELECT * WHERE { ?s ?p ?y FILTER(?y = \"{{year}}\"^^<http://www.w3.org/2001/XMLSchema#gYear>) }"
                        :template-tags {"year" {:name "year" :display-name "Year" :type :number}}
                        :parameters    [{:type "number/=" :target [:variable [:template-tag "year"]] :value 2020}]})]
        (is (= "SELECT * WHERE { ?s ?p ?y FILTER(?y = \"2020\"^^<http://www.w3.org/2001/XMLSchema#gYear>) }" out))
        (is (nil? (tu/sparql-syntax-error out)) out)))
    (testing "a tag at the start of a line, after a line ending in a quote, is not quoted"
      (let [out (subst-tags "SELECT * WHERE { VALUES ?o { \"a\"\n{{x}} } }" {"x" "b"})]
        (is (= "SELECT * WHERE { VALUES ?o { \"a\"\n\"b\" } }" out))
        (is (nil? (tu/sparql-syntax-error out)) out)))
    (testing "a quoted tag on a `#` comment line stays in the comment"
      (let [out (subst-tags "SELECT * WHERE { ?s ?p ?o\n  # FILTER(?o = \"{{x}}\")\n}" {"x" "' || true || '"})]
        (is (= "SELECT * WHERE { ?s ?p ?o\n  # FILTER(?o = \"\"' || true || '\"\")\n}" out))
        (is (nil? (tu/sparql-syntax-error out)) out)))
    (testing "tags next to other tags or to text that is not a quote are fine"
      (let [out (subst-tags "SELECT * WHERE { ?s ?p ?o FILTER(?o IN ({{x}},{{y}})) }" {"x" "a" "y" "b"})]
        (is (= "SELECT * WHERE { ?s ?p ?o FILTER(?o IN (\"a\",\"b\")) }" out))
        (is (nil? (tu/sparql-syntax-error out)) out)))))
