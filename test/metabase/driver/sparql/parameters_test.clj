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

(deftest tag-value-inside-quotes-is-an-error
  (letfn [(subst-tags [q values]
            (subst {:query         q
                    :template-tags (into {} (for [k (keys values)] [k {:name k :display-name k :type :text}]))
                    :parameters    (for [[k v] values :when (some? v)]
                                     {:type "category" :target [:variable [:template-tag k]] :value v})}))
          (subst-x [q value] (subst-tags q {"x" value}))
          (rejected? [q values]
            (try (subst-tags q values) false
                 (catch clojure.lang.ExceptionInfo e
                   (boolean (and (re-find #"variable is inside quotes, or glued to a quote" (ex-message e))
                                 (= :invalid-query (:type (ex-data e))))))))]
    (testing "a text value inside quotes would let it close them and inject"
      (doseq [q ["SELECT * WHERE { ?s ?p ?o FILTER(?o = '{{x}}') }"
                 "SELECT * WHERE { ?s ?p ?o FILTER(?o = \"{{x}}\") }"
                 "SELECT * WHERE { ?s ?p ?o FILTER(?o = \"Hello {{x}}\") }"
                 "SELECT * WHERE { ?s ?p ?o FILTER(?o = \"\"\"a\n{{x}}\"\"\") }"
                 "SELECT * WHERE { ?s ?p ?o FILTER(?o = '''{{x}}''') }"
                 "SELECT * WHERE { ?s ?p ?o FILTER(?o = '''it's {{x}}''') }"
                 "SELECT * WHERE { ?s ?p ?o FILTER(?o = \"\"\"say \"hi {{x}}\"\"\") }"
                 "SELECT * WHERE { ?s ?p ?o FILTER(?o = \"\"\"say \"\"{{x}}\"\"\") }"
                 "SELECT * WHERE { ?s ?p ?o FILTER(?o = \"a\\\"{{x}}\") }"
                 "SELECT * WHERE { ?s ?p ?o FILTER(?o = 'a\\{{x}}') }"
                 "SELECT * WHERE { ?s ?p ?o FILTER(?o<'{{x}}') }"
                 "SELECT * WHERE { ?s ?p ?o FILTER(?o = \"\"{{x}}) }"
                 "SELECT * WHERE { ?s ex:it\\'s ?o FILTER(?o = '{{x}}') }"
                 "SELECT * WHERE { ?s ex:a\\#b \"{{x}}\" }"
                 "SELECT * WHERE { ?s ?p ?o [[FILTER(?o = '{{x}}')]] }"
                 "SELECT * WHERE { ?s <https://example.org/o'b/{{x}}> ?o }"
                 "SELECT * WHERE { ?s ?p ?o FILTER(STRSTARTS(STR(?s), \"{{x}}\")) }"
                 "SELECT * WHERE { ?s ?p ?n FILTER(?n<=5) ?s ?q ?l FILTER(?l = 'x>y' || ?l = '{{x}}') }"]]
        (is (rejected? q {"x" "' || true || '"}) q)))
    (testing "text around a dropped `[[ … ]]` clause, or another value, is lexed as the endpoint reads it"
      (is (rejected? "SELECT * WHERE { ?s ?p \"a\\[[{{y}}]]\" . ?s ?q {{x}} }" {"x" ") || true || (" "y" nil}))
      (is (rejected? "SELECT * WHERE { VALUES ?o { \"[[{{y}}]]\"{{x}} } }" {"x" "v" "y" nil}))
      (is (rejected? "SELECT * WHERE { ?s ?p ?o FILTER(?o = {{a}}{{x}}) }" {"a" "" "x" "v"})))
    (testing "a number or boolean cannot close quotes or brackets, so it is still substituted"
      (letfn [(n-subst [q v]
                (subst {:query         q
                        :template-tags {"n" {:name "n" :display-name "N" :type :number}}
                        :parameters    [{:type "number/=" :target [:variable [:template-tag "n"]] :value v}]}))]
        (doseq [[q v expected] [["SELECT * WHERE { <https://example.org/item/{{n}}> ?p ?o }" 5
                                 "SELECT * WHERE { <https://example.org/item/5> ?p ?o }"]
                                ["SELECT * WHERE { ?s ?p \"{{n}}\" }" -1.5
                                 "SELECT * WHERE { ?s ?p \"-1.5\" }"]
                                ["SELECT * WHERE { ?s ?p ?o FILTER(?o<{{n}}) }" 5
                                 "SELECT * WHERE { ?s ?p ?o FILTER(?o<5) }"]]]
          (let [out (n-subst q v)]
            (is (= expected out))
            (is (nil? (tu/sparql-syntax-error out)) out)))))
    (testing "a tag without a value stays as written, wherever it is"
      (let [q "SELECT * WHERE { ?s ?p '{{x}}' }"]
        (is (= q (subst-x q nil)))
        (is (nil? (tu/sparql-syntax-error q)) q)))
    (testing "a quoted tag in a `[[ … ]]` clause that is dropped does not fail"
      (let [out (subst-tags "SELECT * WHERE { ?s ?p ?o [[FILTER(?o = '{{x}}' && ?p = {{y}})]] }" {"x" "a" "y" nil})]
        (is (= "SELECT * WHERE { ?s ?p ?o  }" out))
        (is (nil? (tu/sparql-syntax-error out)) out)))
    (testing "text that is already closed before the tag, and compact comparisons, are fine"
      (doseq [[q expected] [["SELECT * WHERE { ?s ?p \"it's\" . ?s ?q {{x}} }"
                             "SELECT * WHERE { ?s ?p \"it's\" . ?s ?q \"v\" }"]
                            ["SELECT * WHERE { ?s <https://example.org/o'b> ?o FILTER(?o = {{x}}) }"
                             "SELECT * WHERE { ?s <https://example.org/o'b> ?o FILTER(?o = \"v\") }"]
                            ["SELECT * WHERE {\n# don't quote\n?s ?p {{x}} }"
                             "SELECT * WHERE {\n# don't quote\n?s ?p \"v\" }"]
                            ["SELECT * WHERE {\n# C:\\\\u000A don't\n?s ?p {{x}} }"
                             "SELECT * WHERE {\n# C:\\\\u000A don't\n?s ?p \"v\" }"]
                            ["SELECT * WHERE { ?s ?p \"\"\"say \"hi\" \"\"\" . ?s ?q {{x}} }"
                             "SELECT * WHERE { ?s ?p \"\"\"say \"hi\" \"\"\" . ?s ?q \"v\" }"]
                            ["SELECT * WHERE { VALUES ?o { \"\"\"abc\"\"\"{{x}} } }"
                             "SELECT * WHERE { VALUES ?o { \"\"\"abc\"\"\"\"v\" } }"]
                            ["SELECT * WHERE { ?s ?p \"a\\\\\" . ?s ?q {{x}} }"
                             "SELECT * WHERE { ?s ?p \"a\\\\\" . ?s ?q \"v\" }"]
                            ["SELECT * WHERE { ?s ?p \"\\u00e9\" . ?s ?q {{x}} }"
                             "SELECT * WHERE { ?s ?p \"\\u00e9\" . ?s ?q \"v\" }"]
                            ["SELECT * WHERE { ?s ?p ?d FILTER(?d<{{x}}&&?d>\"a\") }"
                             "SELECT * WHERE { ?s ?p ?d FILTER(?d<\"v\"&&?d>\"a\") }"]
                            ["SELECT * WHERE { ?s ?p ?d FILTER(?d<={{x}}) }"
                             "SELECT * WHERE { ?s ?p ?d FILTER(?d<=\"v\") }"]
                            ["SELECT * WHERE { ?s ?p ?d FILTER(?d<STR({{x}})) }"
                             "SELECT * WHERE { ?s ?p ?d FILTER(?d<STR(\"v\")) }"]]]
        (let [out (subst-x q "v")]
          (is (= expected out))
          (is (nil? (tu/sparql-syntax-error out)) out))))
    (testing "an IRI value holding a `'` is one IRI term"
      (let [out (subst-x "SELECT * WHERE { ?s ?p {{x}} . ?s ?q ?o FILTER(?o = {{x}}) }" "https://example.org/resource/Guns_N'_Roses")]
        (is (= "SELECT * WHERE { ?s ?p <https://example.org/resource/Guns_N'_Roses> . ?s ?q ?o FILTER(?o = <https://example.org/resource/Guns_N'_Roses>) }" out))
        (is (nil? (tu/sparql-syntax-error out)) out)))
    (testing "a list of numbers is as safe as one"
      (let [out (subst {:query         "SELECT * WHERE { ?s ?p ?o FILTER(CONTAINS(\"{{ids}}\", STR(?o))) }"
                        :template-tags {"ids" {:name "ids" :display-name "Ids" :type :number}}
                        :parameters    [{:type "number/=" :target [:variable [:template-tag "ids"]] :value [1 2]}]})]
        (is (= "SELECT * WHERE { ?s ?p ?o FILTER(CONTAINS(\"1, 2\", STR(?o))) }" out))
        (is (nil? (tu/sparql-syntax-error out)) out)))
    (testing "a tag with a value in a `#` comment is substituted, and stays commented out"
      (let [out (subst-x "SELECT * WHERE { ?s ?p ?o\n# FILTER(?o = \"{{x}}\")\n}" "' || true || '")]
        (is (= "SELECT * WHERE { ?s ?p ?o\n# FILTER(?o = \"\"' || true || '\"\")\n}" out))
        (is (nil? (tu/sparql-syntax-error out)) out)))
    (testing "a long literal before the tag does not overflow the stack"
      (let [long-literal (str/join (repeat 100000 "a"))
            out          (subst-x (str "SELECT * WHERE { ?s ?p \"" long-literal "\" . ?s ?q {{x}} }") "v")]
        (is (str/ends-with? out "?s ?q \"v\" }"))
        (is (nil? (tu/sparql-syntax-error out)))))))
