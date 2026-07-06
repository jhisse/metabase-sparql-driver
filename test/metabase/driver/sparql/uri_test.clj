(ns metabase.driver.sparql.uri-test
  "Unit tests for the shared URI/SPARQL-serialization helpers."
  (:require [clojure.test :refer :all]
            [metabase.driver.sparql.uri :as uri]))

(deftest escape-string-test
  (testing "double quotes are escaped"
    (is (= "a\\\"b" (uri/escape-string "a\"b"))))
  (testing "backslashes are escaped, so a trailing one no longer swallows the closing quote"
    (is (= "foo\\\\" (uri/escape-string "foo\\")))
    (is (= "a\\\\b" (uri/escape-string "a\\b"))))
  (testing "newline, carriage return, and tab are escaped to keep the literal single-line"
    (is (= "a\\nb" (uri/escape-string "a\nb")))
    (is (= "a\\rb" (uri/escape-string "a\rb")))
    (is (= "a\\tb" (uri/escape-string "a\tb"))))
  (testing "a plain value is unchanged"
    (is (= "Alice" (uri/escape-string "Alice")))))

(deftest iri-ref-test
  (testing "a well-formed IRI passes through unchanged, wrapped in angle brackets"
    (is (= "<https://data.example.org/Item>"
           (uri/iri-ref "https://data.example.org/Item"))))
  (testing "a `>` in the body is percent-encoded so it cannot close the IRIREF early"
    (is (= "<https://ex.org/a%3Eb>" (uri/iri-ref "https://ex.org/a>b"))))
  (testing "a space is percent-encoded"
    (is (= "<https://ex.org/a%20b>" (uri/iri-ref "https://ex.org/a b"))))
  (testing "other IRIREF-illegal chars are percent-encoded"
    (is (= "<https://ex.org/%22%7B%7D%7C%5E%60%5C>"
           (uri/iri-ref "https://ex.org/\"{}|^`\\")))))
