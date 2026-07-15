(ns metabase.driver.sparql.conversion
  "SPARQL Type Conversion for Metabase SPARQL Driver

   This namespace handles conversion of SPARQL data types to Metabase types.
   Provides functions to map SPARQL types to Metabase base types and convert values."
  (:require [metabase.util.log :as log]))

(def ^:private xsd
  "Base URI of the XSD datatype namespace (same convention as shacl.clj)."
  "http://www.w3.org/2001/XMLSchema#")

;; Single source of truth for the XSD datatype families. The integer, float and
;; boolean families are shared by BOTH type classification
;; (sparql-type->base-type) and value parsing (convert-value): the mixed-type
;; promotion in determine-column-types relies on the two agreeing, since a
;; datatype classified numeric but not parsed numeric would put raw string cells
;; inside a numeric-typed column. The date and datetime families are used by
;; classification only — those values are carried through as their lexical
;; string, so parsing does not reference them.
(def ^:private xsd-integer-datatypes
  #{(str xsd "integer")
    (str xsd "int")
    (str xsd "long")
    (str xsd "short")
    (str xsd "byte")
    (str xsd "nonNegativeInteger")
    (str xsd "positiveInteger")
    (str xsd "nonPositiveInteger")
    (str xsd "negativeInteger")
    (str xsd "unsignedLong")
    (str xsd "unsignedInt")
    (str xsd "unsignedShort")
    (str xsd "unsignedByte")})

(def ^:private xsd-float-datatypes
  #{(str xsd "decimal")
    (str xsd "float")
    (str xsd "double")})

(def ^:private xsd-datetime-datatypes
  #{(str xsd "dateTime")
    (str xsd "gYear")
    (str xsd "gYearMonth")})

(def ^:private xsd-date-datatypes
  #{(str xsd "date")
    (str xsd "gMonthDay")
    (str xsd "gDay")
    (str xsd "gMonth")})

(def ^:private xsd-boolean
  "Single-valued family, named because it is used by BOTH classification and
   parsing — the same must-agree coupling as the numeric sets above."
  (str xsd "boolean"))

(defn sparql-type->base-type
  "Converts a SPARQL type to a Metabase base type.

   A pure lookup, intentionally free of logging: determine-column-types calls
   it once per distinct (type, datatype) pair per column, but callers are free
   to call it per cell.

   Parameters:
     sparql-type - SPARQL type ('uri', 'literal', 'bnode', etc.)
     datatype - Datatype URI (optional)

   Returns:
     Metabase base type (:type/URL, :type/Text, :type/Integer, etc.)"
  [sparql-type datatype]
  (cond
    ;; URIs are represented as text
    (= sparql-type "uri") :type/URL

    ;; Blank nodes
    (= sparql-type "bnode") :type/Text

    ;; Typed literals
    (and datatype
         (or (= sparql-type "typed-literal")
             (= sparql-type "literal")))
    (cond
      (contains? xsd-integer-datatypes datatype)  :type/Integer
      (contains? xsd-float-datatypes datatype)    :type/Float
      (= datatype xsd-boolean)                    :type/Boolean
      (contains? xsd-datetime-datatypes datatype) :type/DateTime
      (contains? xsd-date-datatypes datatype)     :type/Date
      (= datatype (str xsd "time"))               :type/Time
      :else :type/Text)

    ;; Literals with language tags are treated as text
    :else :type/Text))

(defn convert-value
  "Converts a SPARQL value to the appropriate Metabase type.
   
   Parameters:
     binding - SPARQL binding containing :value, :type, and possibly :datatype
   
   Returns:
     Value converted to the appropriate type."
  [binding]
  (let [value (:value binding)
        type-key (:type binding)
        datatype (:datatype binding)
        typed?   (and datatype
                      (or (= type-key "typed-literal")
                          (= type-key "literal")))]
    (cond
      ;; Handle integers (both typed-literal and literal)
      (and typed? (contains? xsd-integer-datatypes datatype))
      (try (Long/parseLong value)
           (catch Exception e
             (log/warn "Failed to convert integer:" value "Error:" (.getMessage e))
             value))

      ;; Handle decimals/floats (both typed-literal and literal)
      (and typed? (contains? xsd-float-datatypes datatype))
      (try (Double/parseDouble value)
           (catch Exception e
             (log/warn "Failed to convert float:" value "Error:" (.getMessage e))
             value))

      ;; Handle booleans (both typed-literal and literal)
      (and typed? (= datatype xsd-boolean))
      (Boolean/parseBoolean value)

      ;; Default case - strings and all other types
      :else value)))

(def ^:private mixed-type-resolution
  "When a column's observed base types differ, the most specific type that can
   represent all of them. Keyed by the EXACT set of observed types: only the
   two-type mixes listed here promote; any other combination — including every
   3+-type mix, e.g. #{:type/Integer :type/Float :type/Text} — deliberately
   degrades to Text,
   the conservative type every cell value renders safely under."
  {#{:type/Integer :type/Float} :type/Float
   #{:type/Date :type/DateTime} :type/DateTime})

(defn determine-column-types
  "Determines column types from the result rows.
   Scans every row instead of a fixed-size sample — a column whose first rows
   are all null or all integers no longer misses a later type flip. The rows
   are already fully materialized in memory; a single pass collects the distinct
   (type, datatype) pairs per column (typically one or two), which are then
   classified — so the cost is one traversal of the present cells, not the
   ~20-branch datatype dispatch per cell.

   Trade-off of the full scan: the column type is now sensitive to every row,
   so a saved question's result_metadata can flip between runs when the
   underlying RDF data gains a divergent value — the price of correctness
   over sampling stability.

   Parameters:
     vars - List of variable names (columns) in the result
     bindings - List of bindings (rows) in the result

   Returns:
     Map associating variable names to Metabase base types"
  [vars bindings]
  (let [;; One pass over the rows, collecting per column key the distinct raw
        ;; (type, datatype) pairs — touching only the cells actually present,
        ;; instead of re-scanning every row once per column.
        pairs-by-key (reduce (fn [acc row]
                               (reduce-kv (fn [acc var-key binding]
                                            (update acc var-key (fnil conj #{})
                                                    [(:type binding) (:datatype binding)]))
                                          acc
                                          row))
                             {}
                             bindings)]
    (reduce (fn [types var-name]
              ;; Classify each column's small pair set (empty when the column is
              ;; absent from every row → Text).
              (let [column-types (into #{}
                                       (map (fn [[t d]] (sparql-type->base-type t d)))
                                       (get pairs-by-key (keyword var-name)))
                    final-type   (cond
                                   (empty? column-types)      :type/Text
                                   (= 1 (count column-types)) (first column-types)
                                   :else (get mixed-type-resolution column-types :type/Text))]
                (assoc types var-name final-type)))
            {}
            vars)))
