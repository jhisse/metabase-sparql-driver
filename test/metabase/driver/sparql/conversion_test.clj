(ns metabase.driver.sparql.conversion-test
  "Unit tests for SPARQL type/value conversion functions."
  (:require [clojure.test :refer :all]
            [metabase.driver.sparql.conversion :as conversion]))

(def ^:private xsd "http://www.w3.org/2001/XMLSchema#")

(deftest sparql-type->base-type-test
  (testing "URI / blank-node node types"
    (is (= :type/URL  (conversion/sparql-type->base-type "uri" nil)))
    (is (= :type/Text (conversion/sparql-type->base-type "bnode" nil))))

  (testing "integer family datatypes"
    (doseq [t ["integer" "int" "long" "short" "byte"
               "nonNegativeInteger" "positiveInteger" "nonPositiveInteger"
               "negativeInteger" "unsignedLong" "unsignedInt"
               "unsignedShort" "unsignedByte"]]
      (is (= :type/Integer (conversion/sparql-type->base-type "typed-literal" (str xsd t)))
          (str t " should map to :type/Integer"))))

  (testing "floating-point family datatypes"
    (doseq [t ["decimal" "float" "double"]]
      (is (= :type/Float (conversion/sparql-type->base-type "literal" (str xsd t)))
          (str t " should map to :type/Float"))))

  (testing "boolean datatype"
    (is (= :type/Boolean (conversion/sparql-type->base-type "typed-literal" (str xsd "boolean")))))

  (testing "DateTime family datatypes"
    (doseq [t ["dateTime" "gYear" "gYearMonth"]]
      (is (= :type/DateTime (conversion/sparql-type->base-type "literal" (str xsd t)))
          (str t " should map to :type/DateTime"))))

  (testing "Date family datatypes"
    (doseq [t ["date" "gMonthDay" "gDay" "gMonth"]]
      (is (= :type/Date (conversion/sparql-type->base-type "literal" (str xsd t)))
          (str t " should map to :type/Date"))))

  (testing "Time datatype"
    (is (= :type/Time (conversion/sparql-type->base-type "literal" (str xsd "time")))))

  (testing "unknown datatype and untagged literals fall back to text"
    (is (= :type/Text (conversion/sparql-type->base-type "literal" (str xsd "anyURI"))))
    (is (= :type/Text (conversion/sparql-type->base-type "literal" nil)))))

(deftest convert-value-test
  (testing "integers are parsed to Long"
    (let [v (conversion/convert-value {:value "42" :type "literal" :datatype (str xsd "integer")})]
      (is (= 42 v))
      (is (instance? Long v))))

  (testing "decimals/doubles are parsed to Double"
    (is (= 3.5 (conversion/convert-value {:value "3.5" :type "typed-literal" :datatype (str xsd "decimal")}))))

  (testing "booleans are parsed"
    (is (true?  (conversion/convert-value {:value "true"  :type "literal" :datatype (str xsd "boolean")})))
    (is (false? (conversion/convert-value {:value "false" :type "literal" :datatype (str xsd "boolean")}))))

  (testing "unparseable numbers fall back to the raw string"
    (is (= "not-a-number"
           (conversion/convert-value {:value "not-a-number" :type "literal" :datatype (str xsd "integer")}))))

  (testing "plain strings and URIs are returned unchanged"
    (is (= "hello" (conversion/convert-value {:value "hello" :type "literal"})))
    (is (= "http://example.org/x" (conversion/convert-value {:value "http://example.org/x" :type "uri"})))))

(deftest determine-column-types-test
  (testing "uniform column types are inferred"
    (is (= {"a" :type/Integer "b" :type/URL}
           (conversion/determine-column-types
            ["a" "b"]
            [{:a {:type "literal" :datatype (str xsd "integer")} :b {:type "uri"}}
             {:a {:type "literal" :datatype (str xsd "integer")} :b {:type "uri"}}]))))

  (testing "mixed types in one column collapse to text"
    (is (= {"a" :type/Text}
           (conversion/determine-column-types
            ["a"]
            [{:a {:type "literal" :datatype (str xsd "integer")}}
             {:a {:type "uri"}}]))))

  (testing "a column with no bindings defaults to text"
    (is (= {"a" :type/Text}
           (conversion/determine-column-types ["a"] [{} {}])))))
