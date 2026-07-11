(ns metabase.driver.sparql.uri
  "Shared URI helpers for the SPARQL driver."
  (:require [clojure.string :as str]))

;; ---------------------------------------------------------------------------
;; Naming context: Default Graph base + namespace-prefix map
;; ---------------------------------------------------------------------------

(def ^:private prefix-separator
  "Separator between a namespace prefix and the local name in shortened names
   (`foaf__name`). Double underscore instead of the RDF-conventional `:`
   because field names feed SPARQL variable sanitization, which would collapse
   `:` to `_` and lose reversibility."
  "__")

(defn parse-prefixes
  "Parse the `namespace-prefixes` connection detail — one `prefix=uri` per
   line — into a vector of `[prefix uri]` pairs ordered by URI length
   descending, so the most specific namespace wins. Ignored: blank lines,
   lines without `=`, prefixes that aren't a simple name (letter followed by
   letters/digits/`_`/`-`) or that contain `__` (it's the separator), and
   duplicate prefixes (first occurrence wins)."
  [s]
  (->> (str/split-lines (or s ""))
       (keep (fn [line]
               (when-let [[_ prefix uri] (re-matches #"([A-Za-z][A-Za-z0-9_-]*)\s*=\s*(\S+)"
                                                     (str/trim line))]
                 (when-not (str/includes? prefix prefix-separator)
                   [prefix uri]))))
       (reduce (fn [acc [p u]]
                 (if (some #(= p (first %)) acc) acc (conj acc [p u])))
               [])
       (sort-by (comp - count second))
       vec))

(defn naming-context
  "Build the URI-naming context consumed by [[shorten-uri]] / [[absolute-uri]] /
   [[foreign-uri?]] from connection details: the Default Graph base plus the
   parsed namespace-prefix pairs."
  [details]
  {:default-graph (:default-graph details)
   :prefixes      (parse-prefixes (:namespace-prefixes details))})

(defn- ->naming
  "Normalize the `base` argument the naming fns accept: either the legacy
   Default-Graph string (or nil) or a full [[naming-context]] map."
  [base]
  (if (map? base) base {:default-graph base}))

(defn absolute-uri
  "Reconstruct a full URI from a (possibly shortened) name. `base` is a
   Default-Graph string or a [[naming-context]] map.

   If `nm` already has a URI scheme it's returned unchanged. A `prefix__local`
   name whose prefix is registered expands to `<prefix-uri>local`; otherwise
   the Default Graph is prepended. Returns `nm` unchanged when `nm` is blank
   or nothing applies."
  [nm base]
  (let [{:keys [default-graph prefixes]} (->naming base)]
    (if (or (str/blank? nm)
            (re-find #"^[A-Za-z][A-Za-z0-9+.-]*:" nm))
      nm
      (or (some (fn [[prefix uri]]
                  (let [head (str prefix prefix-separator)]
                    (when (and (str/starts-with? nm head)
                               (not (str/blank? (subs nm (count head)))))
                      (str uri (subs nm (count head))))))
                prefixes)
          (if (str/blank? default-graph)
            nm
            (str default-graph nm))))))

(defn shorten-uri
  "Shorten `uri` for use as a Metabase table/field name. `base` is a
   Default-Graph string or a [[naming-context]] map.

   The Default Graph wins first: when `uri` starts with it, the prefix is
   stripped (unchanged legacy behavior). Otherwise the registered namespace
   prefixes are tried longest-URI-first, producing `prefix__local`.

   Falls back to the full `uri` whenever the short name could not round-trip
   through [[absolute-uri]]: a blank remainder, a Default-Graph tail that
   itself starts with a registered `prefix__`, or a prefix local name that
   contains `__`."
  [uri base]
  (let [{:keys [default-graph prefixes]} (->naming base)
        ambiguous-tail? (fn [tail]
                          (boolean (some (fn [[prefix _]]
                                           (str/starts-with? tail (str prefix prefix-separator)))
                                         prefixes)))]
    (if-not (string? uri)
      uri
      (if (and (not (str/blank? default-graph))
               (str/starts-with? uri default-graph))
        (let [tail (subs uri (count default-graph))]
          (if (or (str/blank? tail) (ambiguous-tail? tail))
            uri
            tail))
        (or (some (fn [[prefix ns-uri]]
                    (when (and (not (str/blank? ns-uri))
                               (str/starts-with? uri ns-uri))
                      (let [local (subs uri (count ns-uri))]
                        (when-not (or (str/blank? local)
                                      (str/includes? local prefix-separator))
                          (str prefix prefix-separator local)))))
                  prefixes)
            uri)))))

(defn foreign-uri?
  "True when at least one known namespace is configured (the Default Graph or
   a namespace prefix) and `uri` belongs to none of them."
  [uri base]
  (let [{:keys [default-graph prefixes]} (->naming base)
        known (cond-> (mapv second prefixes)
                (not (str/blank? default-graph)) (conj default-graph))]
    (boolean
     (and (string? uri)
          (seq known)
          (not-any? #(str/starts-with? uri %) known)))))

(defn escape-string
  "Escape characters that would break a SPARQL double-quoted string literal.
   Returns the escaped body only; callers wrap it in `\"...\"` (or use
   [[string-literal]]). Backslash is escaped first so the other escapes'
   backslashes are not doubled again."
  [s]
  (-> (str s)
      (str/replace "\\" "\\\\")
      (str/replace "\"" "\\\"")
      (str/replace "\n" "\\n")
      (str/replace "\r" "\\r")
      (str/replace "\t" "\\t")))

(defn string-literal
  "Render `v` as a complete SPARQL double-quoted string literal, escaping the
   body via [[escape-string]] and wrapping it in quotes. The literal counterpart
   to [[iri-ref]]."
  [v]
  (str "\"" (escape-string v) "\""))

(defn iri-shaped?
  "Heuristic: a string that looks like an absolute IRI we should render as
   `<...>` (via [[iri-ref]]) rather than a quoted literal. Accepts `http(s)://`
   and `urn:` shapes; anything else stays a literal. Deliberately narrower than
   [[absolute-uri]]'s scheme check — that one answers \"already has a scheme,
   don't prepend the base\", which is a different question."
  [s]
  (boolean
   (when (string? s)
     (re-find #"^(?:https?://|urn:)" s))))

(defn iri-ref
  "Render `v` as a SPARQL IRIREF `<...>`. Percent-encodes every character the
   IRIREF grammar forbids inside the brackets (the char class below is the
   single source of truth) so a value cannot close the `<...>` early or
   otherwise break/inject the query. IRIREF has no backslash escaping, so
   percent-encoding is the only safe transform. A well-formed IRI passes
   through unchanged."
  [v]
  (let [encoded (str/replace (str v)
                             #"[\x00-\x20<>\"{}|^`\\]"
                             (fn [m] (format "%%%02X" (int (first m)))))]
    (str "<" encoded ">")))
