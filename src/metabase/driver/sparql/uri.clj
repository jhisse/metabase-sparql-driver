(ns metabase.driver.sparql.uri
  "Shared URI helpers for the SPARQL driver."
  (:require [clojure.string :as str]))

(defn absolute-uri
  "Reconstruct a full URI from a (possibly shortened) name.
   If `nm` already has a URI scheme it's returned unchanged; otherwise it's
   treated as relative to `default-graph` (the implicit base prefix) and that
   prefix is prepended. Returns `nm` unchanged when `nm` is blank or
   `default-graph` is blank."
  [nm default-graph]
  (if (or (str/blank? nm)
          (re-find #"^[A-Za-z][A-Za-z0-9+.-]*:" nm)
          (str/blank? default-graph))
    nm
    (str default-graph nm)))

(defn shorten-uri
  "When `uri` starts with `default-graph`, strip that prefix; otherwise return `uri`.
   Blank `default-graph` is treated as a no-op. If stripping would produce a blank
   string (i.e. `uri` equals `default-graph` exactly), the original `uri` is
   returned to keep `:name` non-blank as required by Metabase's field schema."
  [uri default-graph]
  (if (and (not (str/blank? default-graph))
           (string? uri)
           (str/starts-with? uri default-graph))
    (let [tail (subs uri (count default-graph))]
      (if (str/blank? tail) uri tail))
    uri))

(defn foreign-uri?
  "True when `default-graph` is configured and `uri` does not start with it."
  [uri default-graph]
  (and (not (str/blank? default-graph))
       (string? uri)
       (not (str/starts-with? uri default-graph))))

(defn escape-string
  "Escape characters that would break a SPARQL double-quoted string literal.
   Returns the escaped body only; callers wrap it in `\"...\"`. Backslash is
   escaped first so the other escapes' backslashes are not doubled again."
  [s]
  (-> (str s)
      (str/replace "\\" "\\\\")
      (str/replace "\"" "\\\"")
      (str/replace "\n" "\\n")
      (str/replace "\r" "\\r")
      (str/replace "\t" "\\t")))

(defn iri-ref
  "Render `v` as a SPARQL IRIREF `<...>`. Percent-encodes the characters the
   IRIREF grammar forbids inside the brackets — spaces, control chars (0x00-0x20),
   and any of < > \" { } | ^ backtick backslash — so a value cannot close the
   `<...>` early or otherwise break/inject the query. IRIREF has no backslash
   escaping, so percent-encoding is the only safe transform. A well-formed IRI
   passes through unchanged."
  [v]
  (let [encoded (str/replace (str v)
                             #"[\x00-\x20<>\"{}|^`\\]"
                             (fn [m] (format "%%%02X" (int (first m)))))]
    (str "<" encoded ">")))
