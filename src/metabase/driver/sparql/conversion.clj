(ns metabase.driver.sparql.conversion
  "SPARQL Type Conversion for Metabase SPARQL Driver

   This namespace handles conversion of SPARQL data types to Metabase types.
   Provides functions to map SPARQL types to Metabase base types and convert values."
  (:require [metabase.util.log :as log]
            [clojure.string :as str]))

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
      (str/includes? datatype "integer") :type/Integer
      (or
       (str/includes? datatype "decimal")
       (str/includes? datatype "float")
       (str/includes? datatype "double")) :type/Float
      (str/includes? datatype "boolean") :type/Boolean
      (str/includes? datatype "dateTime") :type/DateTime
      (str/includes? datatype "date") :type/Date
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
      ;; Handle typed-literal integers
      (and (= type-key "typed-literal")
           datatype
           (str/includes? datatype "integer"))
      (try (Long/parseLong value)
           (catch Exception e
             (log/warn "Failed to convert integer:" value "Error:" (.getMessage e))
             value))

      ;; Handle typed-literal decimals/floats
      (and (= type-key "typed-literal")
           datatype
           (or (str/includes? datatype "decimal")
               (str/includes? datatype "float")
               (str/includes? datatype "double")))
      (try (Double/parseDouble value)
           (catch Exception e
             (log/warn "Failed to convert float:" value "Error:" (.getMessage e))
             value))

      ;; Handle typed-literal booleans
      (and (= type-key "typed-literal")
           datatype
           (str/includes? datatype "boolean"))
      (Boolean/parseBoolean value)

      ;; Handle regular literal integers
      (and (= type-key "literal")
           datatype
           (str/includes? datatype "integer"))
      (try (Long/parseLong value)
           (catch Exception e
             (log/warn "Failed to convert integer:" value "Error:" (.getMessage e))
             value))

      ;; Handle regular literal decimals/floats
      (and (= type-key "literal")
           datatype
           (or (str/includes? datatype "decimal")
               (str/includes? datatype "float")
               (str/includes? datatype "double")))
      (try (Double/parseDouble value)
           (catch Exception e
             (log/warn "Failed to convert float:" value "Error:" (.getMessage e))
             value))

      ;; Handle regular literal booleans
      (and (= type-key "literal")
           datatype
           (str/includes? datatype "boolean"))
      (Boolean/parseBoolean value)

      ;; Default: return the string value
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
