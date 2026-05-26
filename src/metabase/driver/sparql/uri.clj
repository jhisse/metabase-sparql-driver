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
