(ns metabase.driver.sparql.templates
  "SPARQL Queries for Metabase SPARQL Driver

   This namespace contains predefined SPARQL queries used by the driver
   for various operations such as connection testing and table discovery.
   Each query is optimized for specific use cases."
  (:require [metabase.driver.sparql.uri :as uri]))

(defn connection-test-query
  "Return an ASK query with an empty pattern. Any endpoint that can answer
  queries returns true, so a successful run means it is reachable."
  []
  "ASK { }")

(defn sparql-1-1-bind-version-query
  "Return a SELECT that binds a version string with BIND.

  BIND is SPARQL 1.1, so an endpoint that answers it supports 1.1."
  []
  "SELECT ?version WHERE { BIND(\"SPARQL 1.1\" AS ?version) }")

(defn sparql-1-1-values-version-query
  "Return a SELECT that yields a version string from a VALUES block.

  The fallback when the BIND probe fails: VALUES is also SPARQL 1.1 only."
  []
  "SELECT ?version WHERE { VALUES (?version) { (\"SPARQL 1.1\") } }")

(defn classes-discovery-query
  "Return a SELECT of the RDF classes (`?class`) with their instance counts
  (`?count`), most used first, capped at `limit` (default 1000).

  Each class becomes a table in Metabase."
  ([]
   (classes-discovery-query 1000))
  ([limit]
   (str "SELECT ?class (COUNT(?s) AS ?count) "
        "WHERE { ?s a ?class } "
        "GROUP BY ?class "
        "ORDER BY DESC(?count) "
        "LIMIT " limit)))

(defn class-properties-query
  "Return a SELECT of the properties used by instances of `class-uri`, with
  their use counts (`?count`), most used first.

  Only a sample of `sample-size` instances is scanned (default 1000), so the
  query stays fast on large datasets; at most `limit` properties come back
  (default 20). With `detect-iri?` (default true) it also projects `?isIri`;
  pass false for endpoints that reject that projection."
  ([class-uri]
   (class-properties-query class-uri 20))
  ([class-uri limit]
   (class-properties-query class-uri limit 1000))
  ([class-uri limit sample-size]
   (class-properties-query class-uri limit sample-size true))
  ([class-uri limit sample-size detect-iri?]
   ;; ?isIri is 1 when EVERY sampled value of the property is an IRI node
   ;; (MIN over the indicator): those properties sync as :database-type "uri"
   ;; so equality filters compare against <iri> terms. Mixed or literal-valued
   ;; properties stay 0. IF/aggregates are SPARQL 1.1, which this query
   ;; already requires (COUNT/GROUP BY) — verified on Oxigraph (make smoke) — but
   ;; `detect-iri?` false drops the projection for endpoints that reject it.
   (str "SELECT ?property (COUNT(?instance) AS ?count) "
        (when detect-iri? "(MIN(IF(isIRI(?value), 1, 0)) AS ?isIri) ")
        "WHERE { "
        "  { SELECT ?instance WHERE { "
        ;; The class URI reaches us from synced table names — i.e. from data the
        ;; endpoint returned — so it is not trusted input: iri-ref percent-encodes
        ;; the chars that could close the <...> early and graft clauses onto this
        ;; query (the same guard mbql.clj already applies to class/property IRIs).
        "      ?instance a " (uri/iri-ref class-uri) " "
        "    } LIMIT " sample-size " "
        "  } "
        "  ?instance ?property ?value . "
        "} "
        "GROUP BY ?property "
        "ORDER BY DESC(?count) "
        "LIMIT " limit)))

(defn now-function-support-query
  "Return a SELECT that binds `now()`. An endpoint without the function
  returns no rows or an error."
  []
  "SELECT ?currentDateTime WHERE { BIND(now() AS ?currentDateTime) }")
