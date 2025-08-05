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
  (cond
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
                    ;; Coletamos todos os tipos não-nulos nas primeiras 20 linhas
                    column-types (for [row sample-rows
                                       :let [binding (get row var-key)]
                                       :when binding]
                                   (sparql-type->base-type (:type binding) (:datatype binding)))
                    ;; Se temos tipos diferentes ou nenhum tipo, usamos Text
                    final-type (cond
                                 (empty? column-types) :type/Text
                                 (apply = column-types) (first column-types)
                                 :else :type/Text)]
                (assoc types var-name final-type)))
            {}
            vars)))
