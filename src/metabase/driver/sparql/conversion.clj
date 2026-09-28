(ns metabase.driver.sparql.conversion
  "Map SPARQL result terms to Metabase base types and parse their values into
   Clojure numbers and booleans, per column and per cell."
  (:require [clojure.string :as str]
            [metabase.util.log :as log]))

(def ^:private xsd
  "Base URI of the XSD datatype namespace."
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

;; gYear, gYearMonth, gMonthDay, gDay and gMonth are left out: their values
;; (`1990`, `--12-25`) are not dates or datetimes Metabase can read, so they
;; stay text, as they do in SHACL sync.
(def ^:private xsd-datetime-datatypes
  #{(str xsd "dateTime")})

(def ^:private xsd-date-datatypes
  #{(str xsd "date")})

(def ^:private xsd-boolean
  "Single-valued family, named because it is used by BOTH classification and
   parsing — the same must-agree coupling as the numeric sets above."
  (str xsd "boolean"))

(defn sparql-type->base-type
  "Return the Metabase base type for a SPARQL term type (`\"uri\"`, `\"literal\"`,
   `\"bnode\"`, …) and its optional `datatype` IRI.

   A pure lookup with no logging, so it is safe to call once per cell."
  [sparql-type datatype]
  (cond
    ;; URIs are typed as :type/URL (a Text subtype)
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

    ;; Untyped literals (plain or language-tagged) are treated as text
    :else :type/Text))

(defn convert-value
  "Return the value of a SPARQL result `binding` parsed by its datatype:
   integers to Long (BigInteger past Long's range), decimals and floats to
   Double, booleans to Boolean
   (`true`/`1` and `false`/`0`, the lexical forms of `xsd:boolean`, in any
   case).

   Any other value, or a number or boolean that fails to parse (logged),
   stays the original string."
  [binding]
  (let [value (:value binding)
        type-key (:type binding)
        datatype (:datatype binding)
        typed?   (and datatype
                      (or (= type-key "typed-literal")
                          (= type-key "literal")))]
    (cond
      ;; Handle integers (both typed-literal and literal)
      ;; xsd:integer has no size limit, so a value past Long's range (an ID,
      ;; a population count) becomes a BigInteger rather than staying a
      ;; string in an Integer column.
      (and typed? (contains? xsd-integer-datatypes datatype))
      (try (let [s (str/trim value)]
             (try (Long/parseLong s)
                  (catch NumberFormatException _ (BigInteger. s))))
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
      (case (str/lower-case (str/trim value))
        ("true" "1")  true
        ("false" "0") false
        (do (log/warn "Failed to convert boolean:" value)
            value))

      ;; Default case - strings and all other types
      :else value)))

(def ^:private mixed-type-resolution
  "The type a column takes when its observed base types differ, keyed by the
   exact set of observed types. Only the two-type mixes listed here promote;
   any other combination (every 3+-type mix included) degrades to Text, the
   type every cell value renders safely under."
  {#{:type/Integer :type/Float} :type/Float
   #{:type/Date :type/DateTime} :type/DateTime})

(defn determine-column-types
  "Return a map from each variable name in `vars` to the Metabase base type of
   its values in `bindings`, `:type/Text` when it has none.

   Every row is scanned, not a sample, so a column whose first rows are null
   or integers still catches a later type flip. One pass collects the distinct
   (type, datatype) pairs per column, usually one or two, and only those are
   classified.

   The trade-off: the type depends on every row, so a saved question's
   result_metadata can change between runs when the data gains a divergent
   value."
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
