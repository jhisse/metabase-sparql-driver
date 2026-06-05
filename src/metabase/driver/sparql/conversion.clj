(ns metabase.driver.sparql.conversion
  "SPARQL Type Conversion for Metabase SPARQL Driver

   This namespace handles conversion of SPARQL data types to Metabase types.
   Provides functions to map SPARQL types to Metabase base types and convert values."
  (:require [clojure.string :as str]
            [metabase.util.log :as log]))

;; ---------------------------------------------------------------------------
;; Geometry / WKT detection
;; ---------------------------------------------------------------------------
;; RDF geometry values arrive either as typed literals (Virtuoso's
;; `virtrdf:Geometry`, or the standard `geo:wktLiteral`) or as a plain
;; `xsd:string` whose lexical form is WKT (some stores expose POLYGONs that way).
;; We flag both so the driver can (a) compile equality via STR() and (b) let
;; users pull coordinates out with expressions.

(def geometry-datatypes
  "Datatype IRIs that denote an RDF geometry literal."
  #{"http://www.openlinksw.com/schemas/virtrdf#Geometry" ; Virtuoso's geometry datatype
    "http://www.opengis.net/ont/geosparql#wktLiteral"
    "http://www.opengis.net/ont/sf#wktLiteral"})

(def ^:private wkt-prefix-re
  #"(?i)^\s*(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION|BOX|TRIANGLE|CIRCULARSTRING|TIN)\s*[(ZM]")

(defn geometry-datatype?
  "True when `datatype` (a datatype IRI string) denotes a geometry literal."
  [datatype]
  (boolean (and datatype (contains? geometry-datatypes datatype))))

(defn wkt-string?
  "True when `value` is a string whose lexical form looks like WKT/Virtuoso BOX
   (e.g. `POINT(...)`, `POLYGON((...))`, `BOX(...)`). Catches geometry stored as
   a plain `xsd:string`."
  [value]
  (boolean (and (string? value) (re-find wkt-prefix-re value))))

(defn wkt-kind
  "Classify a WKT/BOX lexical value into a keyword geometry kind
   (`:point`, `:box`, `:polygon`, `:linestring`, `:multipoint`,
   `:multipolygon`, `:multilinestring`, `:geometrycollection`, …) or nil when it
   does not look like geometry. Used at sync to decide which coordinate columns to
   synthesize (only `:point` and `:box` are extractable today)."
  [value]
  (when-let [[_ kw] (and (string? value) (re-find wkt-prefix-re value))]
    (keyword (str/lower-case kw))))

(defn geometry-binding?
  "True when a SPARQL binding (with `:datatype`/`:value`) is a geometry literal,
   by datatype or by WKT-shaped lexical value."
  [binding]
  (boolean (and binding
                (or (geometry-datatype? (:datatype binding))
                    (wkt-string? (:value binding))))))

(defn sparql-type->base-type
  "Converts a SPARQL type to a Metabase base type.
   
   Parameters:
     sparql-type - SPARQL type ('uri', 'literal', 'bnode', etc.)
     datatype - Datatype URI (optional)
   
   Returns:
     Metabase base type (:type/URL, :type/Text, :type/Integer, etc.)"
  [sparql-type datatype]
  (let [base-type (cond
                    ;; URIs are represented as text
                    (= sparql-type "uri") :type/URL

                    ;; Blank nodes
                    (= sparql-type "bnode") :type/Text

                    ;; Typed literals
                    (or (and (= sparql-type "typed-literal") datatype)
                        (and (= sparql-type "literal") datatype))
                    (cond
                      (or (= datatype "http://www.w3.org/2001/XMLSchema#integer")
                          (= datatype "http://www.w3.org/2001/XMLSchema#int")
                          (= datatype "http://www.w3.org/2001/XMLSchema#long")
                          (= datatype "http://www.w3.org/2001/XMLSchema#short")
                          (= datatype "http://www.w3.org/2001/XMLSchema#byte")
                          (= datatype "http://www.w3.org/2001/XMLSchema#nonNegativeInteger")
                          (= datatype "http://www.w3.org/2001/XMLSchema#positiveInteger")
                          (= datatype "http://www.w3.org/2001/XMLSchema#nonPositiveInteger")
                          (= datatype "http://www.w3.org/2001/XMLSchema#negativeInteger")
                          (= datatype "http://www.w3.org/2001/XMLSchema#unsignedLong")
                          (= datatype "http://www.w3.org/2001/XMLSchema#unsignedInt")
                          (= datatype "http://www.w3.org/2001/XMLSchema#unsignedShort")
                          (= datatype "http://www.w3.org/2001/XMLSchema#unsignedByte")) :type/Integer
                      (or (= datatype "http://www.w3.org/2001/XMLSchema#decimal")
                          (= datatype "http://www.w3.org/2001/XMLSchema#float")
                          (= datatype "http://www.w3.org/2001/XMLSchema#double")) :type/Float
                      (= datatype "http://www.w3.org/2001/XMLSchema#boolean") :type/Boolean
                      (or (= datatype "http://www.w3.org/2001/XMLSchema#dateTime")
                          (= datatype "http://www.w3.org/2001/XMLSchema#gYear")
                          (= datatype "http://www.w3.org/2001/XMLSchema#gYearMonth")) :type/DateTime
                      (or (= datatype "http://www.w3.org/2001/XMLSchema#date")
                          (= datatype "http://www.w3.org/2001/XMLSchema#gMonthDay")
                          (= datatype "http://www.w3.org/2001/XMLSchema#gDay")
                          (= datatype "http://www.w3.org/2001/XMLSchema#gMonth")) :type/Date
                      (= datatype "http://www.w3.org/2001/XMLSchema#time") :type/Time
                      :else :type/Text)

                    ;; Literals with language tags are treated as text
                    :else :type/Text)]
    (log/debugf "sparql-type: %s, datatype: %s -> base-type: %s" sparql-type datatype base-type)
    base-type))

(defn convert-value
  "Converts a SPARQL value to the appropriate Metabase type.
   
   Parameters:
     binding - SPARQL binding containing :value, :type, and possibly :datatype
   
   Returns:
     Value converted to the appropriate type."
  [binding]
  (let [value (:value binding)
        type-key (:type binding)
        datatype (:datatype binding)]
    (cond
      ;; Handle integers (both typed-literal and literal)
      (and (or (= type-key "typed-literal")
               (= type-key "literal"))
           datatype
           (or (= datatype "http://www.w3.org/2001/XMLSchema#integer")
               (= datatype "http://www.w3.org/2001/XMLSchema#int")
               (= datatype "http://www.w3.org/2001/XMLSchema#long")
               (= datatype "http://www.w3.org/2001/XMLSchema#short")
               (= datatype "http://www.w3.org/2001/XMLSchema#byte")
               (= datatype "http://www.w3.org/2001/XMLSchema#nonNegativeInteger")
               (= datatype "http://www.w3.org/2001/XMLSchema#positiveInteger")
               (= datatype "http://www.w3.org/2001/XMLSchema#nonPositiveInteger")
               (= datatype "http://www.w3.org/2001/XMLSchema#negativeInteger")
               (= datatype "http://www.w3.org/2001/XMLSchema#unsignedLong")
               (= datatype "http://www.w3.org/2001/XMLSchema#unsignedInt")
               (= datatype "http://www.w3.org/2001/XMLSchema#unsignedShort")
               (= datatype "http://www.w3.org/2001/XMLSchema#unsignedByte")))
      (try (Long/parseLong value)
           (catch Exception e
             (log/warn "Failed to convert integer:" value "Error:" (.getMessage e))
             value))

      ;; Handle decimals/floats (both typed-literal and literal)
      (and (or (= type-key "typed-literal")
               (= type-key "literal"))
           datatype
           (or (= datatype "http://www.w3.org/2001/XMLSchema#decimal")
               (= datatype "http://www.w3.org/2001/XMLSchema#float")
               (= datatype "http://www.w3.org/2001/XMLSchema#double")))
      (try (Double/parseDouble value)
           (catch Exception e
             (log/warn "Failed to convert float:" value "Error:" (.getMessage e))
             value))

      ;; Handle booleans (both typed-literal and literal)
      (and (or (= type-key "typed-literal")
               (= type-key "literal"))
           datatype
           (= datatype "http://www.w3.org/2001/XMLSchema#boolean"))
      (Boolean/parseBoolean value)

      ;; Default case - strings and all other types
      :else value)))

(defn determine-column-types
  "Determines column types based on the first rows of the result.
   Examines up to 20 rows and uses the most generic type when different types exist.
    
   Parameters:
     vars - List of variable names (columns) in the result
     bindings - List of bindings (rows) in the result
    
   Returns:
     Map associating variable names to Metabase base types"
  [vars bindings]
  (let [sample-rows (take 20 bindings)]
    (reduce (fn [types var-name]
              (let [var-key (keyword var-name)
                    ;; Collect all non-null types from the first 20 rows
                    column-types (for [row sample-rows
                                       :let [binding (get row var-key)]
                                       :when binding]
                                   (sparql-type->base-type (:type binding) (:datatype binding)))
                    ;; If we have different types or no type, use Text
                    final-type (cond
                                 (empty? column-types) :type/Text
                                 (apply = column-types) (first column-types)
                                 :else :type/Text)]
                (assoc types var-name final-type)))
            {}
            vars)))
