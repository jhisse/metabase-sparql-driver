(ns metabase.driver.sparql.conversion-test
  "Simple unit tests for SPARQL conversion functions"
  (:require [clojure.test :refer :all]
            [metabase.driver.sparql.conversion :as conversion]))

(deftest test-sparql-type-conversion
  (testing "URI type conversion"
    (is (= :type/URL (conversion/sparql-type->base-type "uri" nil))))

  (testing "Blank node conversion"
    (is (= :type/Text (conversion/sparql-type->base-type "bnode" nil))))

  (testing "Integer type conversion"
    (is (= :type/Integer (conversion/sparql-type->base-type "typed-literal" "http://www.w3.org/2001/XMLSchema#integer")))
    (is (= :type/Integer (conversion/sparql-type->base-type "literal" "http://www.w3.org/2001/XMLSchema#int"))))

  (testing "Float type conversion"
    (is (= :type/Float (conversion/sparql-type->base-type "typed-literal" "http://www.w3.org/2001/XMLSchema#float")))
    (is (= :type/Float (conversion/sparql-type->base-type "literal" "http://www.w3.org/2001/XMLSchema#double"))))

  (testing "Boolean type conversion"
    (is (= :type/Boolean (conversion/sparql-type->base-type "typed-literal" "http://www.w3.org/2001/XMLSchema#boolean")))
    (is (= :type/Boolean (conversion/sparql-type->base-type "literal" "http://www.w3.org/2001/XMLSchema#boolean"))))

  (testing "DateTime type conversion"
    (is (= :type/DateTime (conversion/sparql-type->base-type "typed-literal" "http://www.w3.org/2001/XMLSchema#dateTime")))
    (is (= :type/DateTime (conversion/sparql-type->base-type "literal" "http://www.w3.org/2001/XMLSchema#dateTime"))))

  (testing "Default text type"
    (is (= :type/Text (conversion/sparql-type->base-type "literal" nil)))))