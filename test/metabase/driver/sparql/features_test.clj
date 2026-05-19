(ns metabase.driver.sparql.features-test
  "Unit tests for SPARQL feature detection."
  (:require [clojure.test :refer :all]
            [metabase.driver.sparql.features :as features]))

(deftest database-supports?-test
  (testing "basic aggregations are reported as supported unconditionally"
    (is (true? (features/database-supports? :sparql :basic-aggregations {}))))

  (testing "unknown features fall through to the :default method -> false"
    (is (false? (features/database-supports? :sparql :some-feature-we-do-not-implement {})))
    (is (false? (features/database-supports? :sparql :nested-queries {})))))
