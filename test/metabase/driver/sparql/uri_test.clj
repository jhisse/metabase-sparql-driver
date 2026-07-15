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

;; ---------------------------------------------------------------------------
;; Namespace-prefix naming context
;; ---------------------------------------------------------------------------

(deftest parse-prefixes-test
  (testing "one prefix=uri per line, whitespace tolerated"
    (is (= [["foaf" "http://xmlns.com/foaf/0.1/"]]
           (uri/parse-prefixes "  foaf = http://xmlns.com/foaf/0.1/  "))))
  (testing "pairs come out longest-URI-first so the most specific namespace wins"
    (is (= [["deep" "http://ex.org/a/b/"] ["shallow" "http://ex.org/"]]
           (uri/parse-prefixes "shallow=http://ex.org/\ndeep=http://ex.org/a/b/"))))
  (testing "blank and malformed lines are ignored"
    (is (= [["ok" "http://ex.org/"]]
           (uri/parse-prefixes "\nno-equals-sign\n=http://x/\nok=http://ex.org/\n"))))
  (testing "a line with trailing tokens (e.g. an inline comment) is ignored whole"
    (is (= [] (uri/parse-prefixes "foaf=http://xmlns.com/foaf/0.1/ # people"))))
  (testing "a non-absolute URI value is rejected"
    (is (= [] (uri/parse-prefixes "foaf=foaf/")))
    (is (= [] (uri/parse-prefixes "p=notauri"))))
  (testing "a prefix containing the __ separator is rejected"
    (is (= [] (uri/parse-prefixes "a__b=http://ex.org/"))))
  (testing "duplicate prefixes: first occurrence wins"
    (is (= [["p" "http://first/"]]
           (uri/parse-prefixes "p=http://first/\np=http://x/"))))
  (testing "prefix names colliding under __ (one is the other plus _) are rejected, first kept"
    (is (= [["a" "http://A/"]]
           (uri/parse-prefixes "a=http://A/\na_=http://B/")))
    (is (= [["a_" "http://B/"]]
           (uri/parse-prefixes "a_=http://B/\na=http://A/"))))
  (testing "nil/blank input parses to no prefixes"
    (is (= [] (uri/parse-prefixes nil)))
    (is (= [] (uri/parse-prefixes "")))))

(deftest prefix-shorten-and-expand-test
  (let [naming {:default-graph "https://example.org/"
                :prefixes      (uri/parse-prefixes "foaf=http://xmlns.com/foaf/0.1/")}]
    (testing "a prefix-namespace URI shortens to prefix__local and expands back"
      (is (= "foaf__name" (uri/shorten-uri "http://xmlns.com/foaf/0.1/name" naming)))
      (is (= "http://xmlns.com/foaf/0.1/name" (uri/absolute-uri "foaf__name" naming))))
    (testing "the Default Graph still wins first and round-trips"
      (is (= "Person" (uri/shorten-uri "https://example.org/Person" naming)))
      (is (= "https://example.org/Person" (uri/absolute-uri "Person" naming))))
    (testing "an unrelated URI stays full and expands unchanged (it has a scheme)"
      (is (= "http://other.org/x" (uri/shorten-uri "http://other.org/x" naming)))
      (is (= "http://other.org/x" (uri/absolute-uri "http://other.org/x" naming))))
    (testing "a prefix local name containing __ is not shortened (would not round-trip)"
      (is (= "http://xmlns.com/foaf/0.1/a__b"
             (uri/shorten-uri "http://xmlns.com/foaf/0.1/a__b" naming))))
    (testing "a Default-Graph tail that collides with a registered prefix__ stays full"
      (is (= "https://example.org/foaf__x"
             (uri/shorten-uri "https://example.org/foaf__x" naming))))
    (testing "a Default-Graph tail that looks scheme-shaped stays full (colon is legal in IRI paths)"
      ;; shortening to "has:label" would break the round-trip: absolute-uri's
      ;; scheme check would return it unchanged and the query would emit
      ;; <has:label> instead of the full URI.
      (is (= "https://example.org/has:label"
             (uri/shorten-uri "https://example.org/has:label" naming)))
      (is (= "https://example.org/has:label"
             (uri/shorten-uri "https://example.org/has:label" "https://example.org/"))))
    (testing "an unregistered prefix__ name falls back to Default-Graph expansion"
      (is (= "https://example.org/other__x" (uri/absolute-uri "other__x" naming))))
    (testing "the longest matching namespace URI wins"
      (let [naming {:default-graph nil
                    :prefixes (uri/parse-prefixes "base=http://ex.org/\nsub=http://ex.org/sub/")}]
        (is (= "sub__x" (uri/shorten-uri "http://ex.org/sub/x" naming)))
        (is (= "base__sub" (uri/shorten-uri "http://ex.org/sub" naming)))))
    (testing "with a colliding prefix rejected at parse, the survivor round-trips unambiguously"
      ;; #{a, a_} would make `a___x` ambiguous; parse keeps only `a`, so
      ;; http://B/ is simply unknown and stays a full URI.
      (let [naming {:default-graph nil
                    :prefixes (uri/parse-prefixes "a=http://A/\na_=http://B/")}]
        (is (= "a__x" (uri/shorten-uri "http://A/x" naming)))
        (is (= "http://A/x" (uri/absolute-uri "a__x" naming)))
        (is (= "http://B/x" (uri/shorten-uri "http://B/x" naming)))))))

(deftest legacy-string-base-still-works-test
  (testing "the 2-arity string base behaves exactly as before"
    (is (= "naam" (uri/shorten-uri "https://example.org/naam" "https://example.org/")))
    (is (= "https://example.org/naam" (uri/absolute-uri "naam" "https://example.org/")))
    (is (uri/foreign-uri? "http://other.org/x" "https://example.org/"))
    (is (not (uri/foreign-uri? "https://example.org/x" "https://example.org/")))
    (testing "blank default-graph is a no-op"
      (is (= "http://x/y" (uri/shorten-uri "http://x/y" nil)))
      (is (= "nm" (uri/absolute-uri "nm" nil)))
      (is (not (uri/foreign-uri? "http://x/y" nil))))))

(deftest foreign-uri?-with-prefixes-test
  (let [naming {:default-graph "https://example.org/"
                :prefixes      (uri/parse-prefixes "foaf=http://xmlns.com/foaf/0.1/")}]
    (testing "prefix-namespace URIs count as local"
      (is (not (uri/foreign-uri? "http://xmlns.com/foaf/0.1/name" naming)))
      (is (not (uri/foreign-uri? "https://example.org/x" naming)))
      (is (uri/foreign-uri? "http://other.org/x" naming))))
  (testing "prefixes alone (no Default Graph) also define what is local"
    (let [naming {:default-graph nil
                  :prefixes      (uri/parse-prefixes "foaf=http://xmlns.com/foaf/0.1/")}]
      (is (not (uri/foreign-uri? "http://xmlns.com/foaf/0.1/name" naming)))
      (is (uri/foreign-uri? "http://other.org/x" naming)))))
