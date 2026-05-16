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
