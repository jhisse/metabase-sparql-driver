(ns metabase.driver.sparql.templates
  "SPARQL Queries for Metabase SPARQL Driver

   This namespace contains predefined SPARQL queries used by the driver
   for various operations such as connection testing and table discovery.
   Each query is optimized for specific use cases.")

(defn connection-test-query
  "Simple query to test connectivity with a SPARQL endpoint.

   Returns:
     A single row with a single column named ?ping containing the value 'pong'

   Usage:
     Used by the driver/can-connect? method to check endpoint availability
     A successful execution of this query indicates the endpoint is accessible"
  []
  "ASK { }")

(defn sparql-1-1-bind-version-query
  "Returns a SPARQL 1.1 query that uses the BIND clause to provide a version string.
  
  This query is useful to test whether the SPARQL endpoint supports SPARQL 1.1 features, 
  specifically the BIND operator, which is not available in SPARQL 1.0.
  
  Returns:
    A string containing a SPARQL SELECT query with the version string using BIND.
  
  Usage:
    Use this query to check SPARQL 1.1 compatibility."
  []
  "SELECT ?version WHERE { BIND(\"SPARQL 1.1\" AS ?version) }")

(defn sparql-1-1-values-version-query
  "Returns a SPARQL 1.1 query that uses the VALUES clause to provide a version string.
  
  This query is useful as fallback to test whether the SPARQL endpoint supports SPARQL 1.1 features, 
  specifically the VALUES operator, which is not available in SPARQL 1.0.
  
  Returns:
    A string containing a SPARQL SELECT query with the version string using VALUES.
  
  Usage:
    Use this query as a fallback to check SPARQL 1.1 compatibility."
  []
  "SELECT ?version WHERE { VALUES (?version) { (\"SPARQL 1.1\") } }")

(defn classes-discovery-query
  "Returns the SPARQL query to discover RDF classes, with optional limit.

   Parameters:
     limit - Optional integer limiting the number of classes returned (default: 1000)

   Returns:
     A list of RDF classes (?class) with their instance counts (?count)
     Limited to the top classes by instance count

   Usage:
     Used by the driver/describe-database method to discover available 'tables'
     Each RDF class is treated as a table in Metabase's data model
     The count helps identify the most significant classes in the dataset"
  ([]
   (classes-discovery-query 1000))
  ([limit]
   (str "SELECT ?class (COUNT(?s) AS ?count) "
        "WHERE { ?s a ?class } "
        "GROUP BY ?class "
        "ORDER BY DESC(?count) "
        "LIMIT " limit)))

(defn class-properties-query
  "Returns a SPARQL query to discover properties of an RDF class.
  
  This query finds the most frequently used properties by instances of a specific RDF class,
  ordered by frequency of occurrence.
  
  Parameters:
    class-uri - URI of the RDF class to discover properties for
    limit - Optional integer limiting the number of properties returned (default: 20)
  
  Returns:
    A string containing a SPARQL SELECT query that returns the most common properties 
    used by instances of the class, along with their occurrence count.
  
  Usage:
    Used by the driver/describe-table method to discover available 'fields' for a 'table'."
  ([class-uri]
   (class-properties-query class-uri 20))
  ([class-uri limit]
   (str "SELECT ?property (COUNT(?instance) AS ?count) WHERE { "
        "?instance a <" class-uri "> ; "
        "?property ?value . "
        "} GROUP BY ?property "
        "ORDER BY DESC(?count) "
        "LIMIT " limit)))

(defn now-function-support-query
  "Returns a SPARQL query that tests if the endpoint supports the now() function.
  
  This query attempts to use the SPARQL now() function to get the current datetime.
  If the endpoint supports this function, it will return a result with the current datetime.
  If not, it will either return no results or throw an error.
  
  Returns:
    A string containing a SPARQL SELECT query using the now() function.
  
  Usage:
    Used to test if the SPARQL endpoint supports the now() temporal function."
  []
  "SELECT ?currentDateTime WHERE { BIND(now() AS ?currentDateTime) }")
