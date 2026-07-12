(ns metabase.driver.sparql.uri
  "Shared URI helpers for the SPARQL driver."
  (:require [clojure.string :as str]
            [metabase.util.log :as log]))

;; ---------------------------------------------------------------------------
;; Naming context: Default Graph base + namespace-prefix map
;; ---------------------------------------------------------------------------

(def ^:private prefix-separator
  "Separator between a namespace prefix and the local name in shortened names
   (`foaf__name`). Double underscore instead of the RDF-conventional `:`
   because field names feed SPARQL variable sanitization, which would collapse
   `:` to `_` and lose reversibility."
  "__")

(def ^:private scheme-pattern
  "Matches a name that already carries a URI scheme (RFC-3986 shape; the char
   class covers upper and lower case)."
  #"^[A-Za-z][A-Za-z0-9+.-]*:")

(def ^:private prefixed-name-pattern
  "Matches a name shaped like `prefix__…` (prefix charset per [[parse-prefix-line]])."
  #"^[A-Za-z][A-Za-z0-9_-]*__")

(defn- prefix-head
  "The `prefix__` head a shortened name starts with."
  [prefix]
  (str prefix prefix-separator))

(defonce ^:private warned
  ;; Distinct keys already warned about. parse-prefixes runs on every
  ;; naming-context build (several times per query compile) and absolute-uri
  ;; runs per field per compile, so an unconditional warn would repeat the
  ;; same message forever; one warning per process per offending string is
  ;; diagnostic enough. Bounded by the tiny set of distinct bad inputs.
  (atom #{}))

(defn- warn-once!
  "Log `msg` at WARN, at most once per process for a given `k`."
  [k msg]
  (when-not (contains? @warned k)
    (swap! warned conj k)
    (log/warn msg)))

(defn- parse-prefix-line
  "Parse one non-blank `prefix=uri` line into a `[prefix uri]` pair, or nil
   with a logged warning — a silently dropped line makes prefixed fields
   vanish from sync with no diagnostic."
  [line]
  (if-let [[_ prefix uri] (re-matches #"([A-Za-z][A-Za-z0-9_-]*)\s*=\s*(\S+)" line)]
    (cond
      (str/includes? prefix prefix-separator)
      (do (warn-once! line (format "[sparql.uri] Ignoring namespace prefix %s: prefixes cannot contain \"__\"" prefix))
          nil)

      (not (re-find scheme-pattern uri))
      (do (warn-once! line (format "[sparql.uri] Ignoring namespace prefix %s: %s is not an absolute URI" prefix uri))
          nil)

      :else [prefix uri])
    (do (warn-once! line (format "[sparql.uri] Ignoring malformed namespace-prefixes line: %s" line))
        nil)))

(defn parse-prefixes
  "Parse the `namespace-prefixes` connection detail — one `prefix=uri` per
   line — into a vector of `[prefix uri]` pairs ordered by URI length
   descending, so the most specific namespace wins. Ignored WITH a logged
   warning: lines without `=` or with trailing tokens, prefixes that aren't a
   simple name (letter followed by letters/digits/`_`/`-`) or that contain
   `__` (it's the separator), and non-absolute URI values. Blank lines are
   skipped silently; duplicate prefixes keep the first occurrence."
  [s]
  (->> (str/split-lines (or s ""))
       (map str/trim)
       (remove str/blank?)
       (keep parse-prefix-line)
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
  "Normalize the `base` argument the naming fns accept: a [[naming-context]]
   map, or — legacy form kept for tests/back-compat, no production caller —
   a bare Default-Graph string (or nil)."
  [base]
  (if (map? base) base {:default-graph base}))

(defn absolute-uri
  "Reconstruct a full URI from a (possibly shortened) name. `base` is a
   [[naming-context]] map (legacy: a bare Default-Graph string).

   If `nm` already has a URI scheme it's returned unchanged. A `prefix__local`
   name whose prefix is registered expands to `<prefix-uri>local`; otherwise
   the Default Graph is prepended. Returns `nm` unchanged when `nm` is blank
   or nothing applies.

   A `prefix__`-shaped name that matches NO registered prefix logs a warning
   before falling back to the Default Graph: it usually means the prefix map
   changed after the last sync, and the fallback URI queries nothing."
  [nm base]
  (let [{:keys [default-graph prefixes]} (->naming base)]
    (if (or (str/blank? nm)
            (re-find scheme-pattern nm))
      nm
      (or (some (fn [[prefix uri]]
                  (let [head (prefix-head prefix)]
                    (when (and (str/starts-with? nm head)
                               (not (str/blank? (subs nm (count head)))))
                      (str uri (subs nm (count head))))))
                prefixes)
          (do
            (when (and (seq prefixes)
                       (re-find prefixed-name-pattern nm))
              (warn-once! nm
                          (format (str "[sparql.uri] Name %s looks namespace-prefixed but matches no configured "
                                       "prefix; falling back to the Default Graph. If a prefix was changed or "
                                       "removed, re-sync the database. (A plain Default-Graph name containing "
                                       "\"__\" also triggers this — then it is safe to ignore.)")
                                  nm)))
            (if (str/blank? default-graph)
              nm
              (str default-graph nm)))))))

(defn shorten-uri
  "Shorten `uri` for use as a Metabase table/field name. `base` is a
   [[naming-context]] map (legacy: a bare Default-Graph string).

   The Default Graph wins first: when `uri` starts with it, the prefix is
   stripped (unchanged legacy behavior). Otherwise the registered namespace
   prefixes are tried longest-URI-first, producing `prefix__local`.

   Falls back to the full `uri` whenever the short name could not round-trip
   through [[absolute-uri]]: a blank remainder, a Default-Graph tail that
   itself starts with a registered `prefix__` or that looks like it carries a
   URI scheme (a colon is legal in IRI path segments — `has:label` would be
   returned unchanged by [[absolute-uri]]'s scheme check), or a prefix local
   name that contains `__`."
  [uri base]
  (let [{:keys [default-graph prefixes]} (->naming base)
        ambiguous-tail? (fn [tail]
                          (boolean (or (re-find scheme-pattern tail)
                                       (some (fn [[prefix _]]
                                               (str/starts-with? tail (prefix-head prefix)))
                                             prefixes))))]
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
                          (str (prefix-head prefix) local)))))
                  prefixes)
            uri)))))

(defn foreign-uri?
  "True when at least one known namespace is configured (the Default Graph or
   a namespace prefix) and `uri` belongs to none of them. `base` is a
   [[naming-context]] map (legacy: a bare Default-Graph string)."
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
