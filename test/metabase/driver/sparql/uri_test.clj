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
  (testing "a backslash immediately before a newline: backslash doubled first, then newline escaped"
    ;; input is one backslash + a real newline; correct output is 3 backslashes + n
    ;; (\\ from doubling, then \n from the newline). Reversing the two replace steps
    ;; would corrupt this into 4 backslashes + n.
    (is (= "\\\\\\n" (uri/escape-string (str "\\" "\n")))))
  (testing "a plain value is unchanged"
    (is (= "Alice" (uri/escape-string "Alice")))))

(deftest string-literal-test
  (testing "escapes the body and wraps it in double quotes"
    (is (= "\"a\\\"b\"" (uri/string-literal "a\"b")))
    (is (= "\"plain\"" (uri/string-literal "plain")))))

(deftest iri-shaped?-test
  (testing "http(s) and urn shapes are IRIs"
    (is (uri/iri-shaped? "https://ex.org/x"))
    (is (uri/iri-shaped? "http://ex.org/x"))
    (is (uri/iri-shaped? "urn:isbn:123")))
  (testing "plain strings and non-strings are not"
    (is (not (uri/iri-shaped? "note: see above")))
    (is (not (uri/iri-shaped? "C:\\path")))
    (is (not (uri/iri-shaped? "Alice")))
    (is (not (uri/iri-shaped? 42)))
    (is (not (uri/iri-shaped? nil)))))

(deftest iri-ref-test
  (testing "a well-formed IRI passes through unchanged, wrapped in angle brackets"
    (is (= "<https://data.example.org/Item>"
           (uri/iri-ref "https://data.example.org/Item"))))
  (testing "a `>` in the body is percent-encoded so it cannot close the IRIREF early"
    (is (= "<https://ex.org/a%3Eb>" (uri/iri-ref "https://ex.org/a>b"))))
  (testing "a `<` in the body is percent-encoded too"
    (is (= "<https://ex.org/a%3Cb>" (uri/iri-ref "https://ex.org/a<b"))))
  (testing "a space is percent-encoded"
    (is (= "<https://ex.org/a%20b>" (uri/iri-ref "https://ex.org/a b"))))
  (testing "a control char (newline) is percent-encoded"
    (is (= "<a%0Ab>" (uri/iri-ref "a\nb"))))
  (testing "already percent-encoded input is not double-encoded (`%` is left alone)"
    (is (= "<https://ex.org/a%20b>" (uri/iri-ref "https://ex.org/a%20b"))))
  (testing "other IRIREF-illegal chars are percent-encoded"
    (is (= "<https://ex.org/%22%7B%7D%7C%5E%60%5C>"
           (uri/iri-ref "https://ex.org/\"{}|^`\\")))))
