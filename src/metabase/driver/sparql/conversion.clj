;; SPARQL Type Conversion for Metabase SPARQL Driver
;;
;; This namespace handles conversion of SPARQL data types to Metabase types.
;; Provides functions to map SPARQL types to Metabase base types and convert values.
(ns metabase.driver.sparql.conversion
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
    (and (= sparql-type "typed-literal") datatype)
    (cond
      (str/includes? datatype "integer") :type/Integer
      (str/includes? datatype "decimal") :type/Float
      (str/includes? datatype "float") :type/Float
      (str/includes? datatype "double") :type/Float
      (str/includes? datatype "boolean") :type/Boolean
      (str/includes? datatype "dateTime") :type/DateTime
      (str/includes? datatype "date") :type/Date
      :else :type/Text)

    ;; Regular literals with explicit datatypes
    (and (= sparql-type "literal") datatype)
    (cond
      (str/includes? datatype "integer") :type/Integer
      (str/includes? datatype "decimal") :type/Float
      (str/includes? datatype "float") :type/Float
      (str/includes? datatype "double") :type/Float
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
