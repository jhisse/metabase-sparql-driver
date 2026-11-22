(ns metabase.driver.sparql.conversion
  "Map SPARQL result terms to Metabase base types and parse their values into
   Clojure numbers and booleans, per column and per cell."
  (:require [clojure.string :as str]
            [metabase.util.log :as log]))

(def ^:private xsd
  "Base URI of the XSD datatype namespace."
  "http://www.w3.org/2001/XMLSchema#")

;; Single source of truth for the XSD datatype families. The integer, decimal,
;; float and boolean families are shared by BOTH type classification
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

(def ^:private xsd-decimal
  "Exact decimals, parsed to BigDecimal so no digit is lost."
  (str xsd "decimal"))

(def ^:private xsd-float-datatypes
  #{(str xsd "float")
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
      (= datatype xsd-decimal)                    :type/Decimal
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
   integers to Long (BigInteger past Long's range), decimals to BigDecimal,
   floats and doubles to Double (`INF` and `-INF` to the infinities), booleans
   to Boolean
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
             (log/warn "Failed to convert integer:" value "Error:" (Exception/.getMessage e))
             value))

      ;; Decimals keep every digit: a Double holds only 15-17 significant ones
      (and typed? (= datatype xsd-decimal))
      (try (BigDecimal. (str/trim value))
           (catch Exception e
             (log/warn "Failed to convert decimal:" value "Error:" (Exception/.getMessage e))
             value))

      ;; Floats and doubles; XSD writes the infinities INF, +INF and -INF
      (and typed? (contains? xsd-float-datatypes datatype))
      (try (case (str/trim value)
             ("INF" "+INF") Double/POSITIVE_INFINITY
             "-INF"         Double/NEGATIVE_INFINITY
             (Double/parseDouble value))
           (catch Exception e
             (log/warn "Failed to convert float:" value "Error:" (Exception/.getMessage e))
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

(defn- mixed-type-resolution
  "Return the type a column takes when its observed base `types` differ: the
   widest one when they are all numeric (Float, else Decimal), DateTime for
   dates mixed with datetimes, else Text, the type every cell value renders
   safely under."
  [types]
  (cond
    (every? #{:type/Integer :type/Decimal :type/Float} types)
    (if (contains? types :type/Float) :type/Float :type/Decimal)

    (= types #{:type/Date :type/DateTime}) :type/DateTime

    :else :type/Text))

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
                                   :else (mixed-type-resolution column-types))]
                (assoc types var-name final-type)))
            {}
            vars)))
