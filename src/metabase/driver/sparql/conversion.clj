(ns metabase.driver.sparql.conversion
  "SPARQL Type Conversion for Metabase SPARQL Driver

   This namespace handles conversion of SPARQL data types to Metabase types.
   Provides functions to map SPARQL types to Metabase base types and convert values."
  (:require [metabase.util.log :as log]))

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

(def ^:private mixed-type-resolution
  "When a column's observed base types differ, the most specific type that can
   represent all of them. Any combination not listed here degrades to Text."
  {#{:type/Integer :type/Float} :type/Float
   #{:type/Date :type/DateTime} :type/DateTime})

(defn determine-column-types
  "Determines column types from the result rows.
   Scans every row (the bindings are already fully materialized in memory, so
   this adds no I/O) instead of a fixed-size sample — a column whose first rows
   are all null or all integers no longer misses a later type flip.

   Parameters:
     vars - List of variable names (columns) in the result
     bindings - List of bindings (rows) in the result

   Returns:
     Map associating variable names to Metabase base types"
  [vars bindings]
  (reduce (fn [types var-name]
            (let [var-key (keyword var-name)
                  ;; Distinct non-null base types observed across all rows
                  column-types (into #{}
                                     (keep (fn [row]
                                             (when-let [binding (get row var-key)]
                                               (sparql-type->base-type (:type binding)
                                                                       (:datatype binding)))))
                                     bindings)
                  final-type (cond
                               (empty? column-types)      :type/Text
                               (= 1 (count column-types)) (first column-types)
                               :else (get mixed-type-resolution column-types :type/Text))]
              (assoc types var-name final-type)))
          {}
          vars))
