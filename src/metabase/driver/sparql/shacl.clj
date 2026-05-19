(ns metabase.driver.sparql.shacl
  "SHACL-based metadata sync for the SPARQL driver.

   Given a URL serving a SHACL document in Turtle, this namespace:
   - fetches and parses the document,
   - extracts shapes (one per RDF class / Metabase table),
   - extracts property shapes (one per Metabase column),
   - resolves foreign-key relationships declared via `sh:class`,
   - honors a small `metabase:` vocabulary for sync-time overrides
     (`hide`, `semanticType`, `displayValueProperty`).

   The public entry point is [[metadata]] which returns a fully-resolved
   intermediate description that [[metabase.driver.sparql.database]] turns
   into the maps Metabase's sync interface expects. Results are cached
   per-URL with a short TTL so a single sync run only fetches once."
  (:require [clj-http.client :as http]
            [clojure.string :as str]
            [metabase.util.log :as log])
  (:import (java.io StringReader)
           (org.eclipse.rdf4j.model BNode IRI Literal Resource Statement Value)
           (org.eclipse.rdf4j.rio RDFFormat Rio)))

(def ^:private sh   "http://www.w3.org/ns/shacl#")
(def ^:private rdf  "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
(def ^:private xsd  "http://www.w3.org/2001/XMLSchema#")
(def ^:private mb   "https://data.metabase.com/")

(def ^:private lang-string-datatype (str rdf "langString"))

(def ^:private datatype->base-type
  "Mapping from XSD datatype IRI suffix to Metabase base-type keyword."
  {"string"             :type/Text
   "normalizedString"   :type/Text
   "token"              :type/Text
   "language"           :type/Text
   "anyURI"             :type/Text
   "boolean"            :type/Boolean
   "integer"            :type/Integer
   "int"                :type/Integer
   "long"               :type/Integer
   "short"              :type/Integer
   "byte"               :type/Integer
   "unsignedInt"        :type/Integer
   "unsignedLong"       :type/Integer
   "unsignedShort"      :type/Integer
   "unsignedByte"       :type/Integer
   "nonNegativeInteger" :type/Integer
   "positiveInteger"    :type/Integer
   "decimal"            :type/Float
   "float"              :type/Float
   "double"             :type/Float
   "date"               :type/Date
   "dateTime"           :type/DateTimeWithTZ
   "dateTimeStamp"      :type/DateTimeWithTZ
   "time"               :type/Time})

(defn- term
  "Tag an RDF4J `Value` as a small Clojure map keyed by `:type`."
  [^Value v]
  (cond
    (instance? IRI v)     {:type :iri :value (.stringValue v)}
    (instance? BNode v)   {:type :bnode :id (.getID ^BNode v)}
    (instance? Literal v) (let [^Literal lit v]
                            {:type     :literal
                             :value    (.getLabel lit)
                             :datatype (.stringValue (.getDatatype lit))
                             :lang     (let [opt (.getLanguage lit)]
                                         (when (.isPresent opt) (.get opt)))})
    :else                 {:type :unknown :value (str v)}))

(def ^:private default-connect-timeout-ms 10000)
(def ^:private default-socket-timeout-ms  30000)
(def ^:private default-max-bytes (* 10 1024 1024))

(defn fetch-shacl
  "GET the SHACL document at `url`. Returns the response body as a string.

   `opts` may supply `:connect-timeout-ms`, `:socket-timeout-ms` and
   `:max-bytes`; each falls back to a built-in default. `http` URLs are
   accepted alongside `https` so a local endpoint can be used while testing.

   Throws an `ex-info` on non-2xx responses or when the body exceeds the
   configured size cap."
  ([url] (fetch-shacl url nil))
  ([url {:keys [connect-timeout-ms socket-timeout-ms max-bytes]}]
   (let [connect-ms (or connect-timeout-ms default-connect-timeout-ms)
         socket-ms  (or socket-timeout-ms default-socket-timeout-ms)
         max-bytes  (or max-bytes default-max-bytes)]
     (log/infof "[shacl] Fetching SHACL document from %s" url)
     (let [resp (http/get url {:headers            {"Accept" "text/turtle"}
                               :throw-exceptions   false
                               :connection-timeout connect-ms
                               :socket-timeout     socket-ms
                               :as                 :string})]
       (if (= 200 (:status resp))
         (let [body  (:body resp)
               bytes (alength (.getBytes ^String body "UTF-8"))]
           (when (> bytes max-bytes)
             (throw (ex-info (format "SHACL document from %s is %d bytes, exceeding the %d-byte limit"
                                     url bytes max-bytes)
                             {:url url :bytes bytes :max-bytes max-bytes})))
           body)
         (throw (ex-info (format "Failed to fetch SHACL document from %s (status %s)"
                                 url (:status resp))
                         {:url url :status (:status resp)})))))))

(def ^:private no-contexts
  "Empty array for the trailing `Resource...` varargs on `Rio/parse`. Clojure's
   reflector doesn't synthesize an empty varargs array, so we materialize one."
  (make-array Resource 0))

(defn parse-turtle
  "Parse Turtle `text` into a vector of `[s p o]` triples, where each term is a
   small map (see [[term]])."
  [text base-iri]
  (let [model (Rio/parse (StringReader. text) ^String (or base-iri "") RDFFormat/TURTLE
                         ^"[Lorg.eclipse.rdf4j.model.Resource;" no-contexts)]
    (mapv (fn [^Statement stmt]
            [(term (.getSubject stmt))
             (term (.getPredicate stmt))
             (term (.getObject stmt))])
          model)))

(defn- iri? [t] (= :iri (:type t)))
(defn- literal? [t] (= :literal (:type t)))
(defn- iri= [t v] (and (iri? t) (= (:value t) v)))

(defn- index-spo
  "Build an index `subject → predicate-iri → [object ...]` from a flat triple seq.
   Subjects keyed by their `term` map, predicates by IRI string."
  [triples]
  (reduce (fn [acc [s p o]]
            (if (iri? p)
              (update-in acc [s (:value p)] (fnil conj []) o)
              acc))
          {}
          triples))

(defn- single
  "Return the first object for `subject`/`pred-iri`, or `nil`."
  [spo subject pred-iri]
  (first (get-in spo [subject pred-iri])))

(defn- objects
  "All objects for `subject`/`pred-iri`, or `()` if none."
  [spo subject pred-iri]
  (get-in spo [subject pred-iri]))

(defn- literal-truthy?
  "True for a Literal that's a boolean `true` or a string `true`/`1`."
  [t]
  (and (literal? t)
       (let [v (some-> (:value t) str/lower-case)]
         (or (= v "true") (= v "1")))))

(defn- xsd-base-type [datatype-iri]
  (when (and (string? datatype-iri) (str/starts-with? datatype-iri xsd))
    (datatype->base-type (subs datatype-iri (count xsd)))))

(defn- semantic-type-from-datatype [datatype-iri]
  (when (= datatype-iri (str xsd "anyURI"))
    :type/URL))

(defn- coerce-semantic-type
  "Accept either a literal like \"type/URL\" or `:type/URL` and return the
   keyword, or `nil` for blank/unrecognized input."
  [t]
  (when t
    (let [s (when (or (literal? t) (iri? t)) (:value t))]
      (when-not (str/blank? s)
        (cond
          (str/starts-with? s "type/") (keyword s)
          (str/starts-with? s ":")     (keyword (subs s 1))
          :else                        (keyword s))))))

(defn- parse-long-literal [t]
  (when (literal? t)
    (try (Long/parseLong (:value t)) (catch Exception _ nil))))

(defn- pick-localized
  "From a sequence of object terms (some literals, some IRIs), pick the best
   string value for the configured `lang`. Preference order:
     1. literal with `:lang` matching `lang`,
     2. literal with no language tag,
     3. first remaining literal.
   Returns the string `:value`, or `nil` when no literal is available."
  [terms lang]
  (let [lits          (filter literal? terms)
        match-lang    (when-not (str/blank? lang)
                        (first (filter #(= lang (:lang %)) lits)))
        untagged      (first (filter #(str/blank? (:lang %)) lits))
        any           (first lits)]
    (some-> (or match-lang untagged any) :value)))

(defn- property-shape
  "Extract one property descriptor from the property-shape node `prop-node`.
   `lang` is the configured default language tag (may be nil/blank) used to
   pick the best `sh:name` / `sh:description` literal."
  [spo prop-node lang]
  (let [path        (single spo prop-node (str sh "path"))
        datatype    (single spo prop-node (str sh "datatype"))
        target-cls  (single spo prop-node (str sh "class"))
        name-text   (pick-localized (objects spo prop-node (str sh "name")) lang)
        desc-text   (pick-localized (objects spo prop-node (str sh "description")) lang)
        order-lit   (single spo prop-node (str sh "order"))
        min-count   (single spo prop-node (str sh "minCount"))
        mb-hide     (single spo prop-node (str mb "hide"))
        mb-sem      (single spo prop-node (str mb "semanticType"))
        mb-display  (single spo prop-node (str mb "displayValueProperty"))
        datatype-iri (when (iri? datatype) (:value datatype))
        lang-string? (= datatype-iri lang-string-datatype)
        base-type   (or (xsd-base-type datatype-iri)
                        (when lang-string? :type/Text)
                        (when (iri? target-cls) :type/Text)
                        :type/Text)
        sem-type    (or (coerce-semantic-type mb-sem)
                        (when (iri? target-cls) :type/FK)
                        (semantic-type-from-datatype datatype-iri))
        descr       (->> [name-text
                          desc-text
                          (when (iri? mb-display)
                            (str "Display via " (:value mb-display)))]
                         (remove str/blank?)
                         (str/join " — "))]
    (when (iri? path)
      {:property-uri      (:value path)
       :base-type         base-type
       :semantic-type     sem-type
       :description       (when-not (str/blank? descr) descr)
       :order             (parse-long-literal order-lit)
       :fk-target-class   (when (iri? target-cls) (:value target-cls))
       :display-value-property (when (iri? mb-display) (:value mb-display))
       :database-required (boolean (and (literal? min-count)
                                        (some-> (:value min-count)
                                                Long/parseLong
                                                pos?)))
       :lang-string?      lang-string?
       :hidden?           (literal-truthy? mb-hide)})))

(defn- shape
  "Extract one shape descriptor from the shape node `shape-node`. Returns nil
   for shapes without an `sh:targetClass` (we only sync class-targeted shapes).

   `:parent-shape-iris` carries the IRIs referenced via `sh:node` so that
   [[resolve-inheritance]] can flatten parent properties into the child."
  [spo shape-node lang]
  (let [target-cls   (single spo shape-node (str sh "targetClass"))
        mb-hide      (single spo shape-node (str mb "hide"))
        desc-text    (pick-localized (objects spo shape-node (str sh "description")) lang)
        prop-nodes   (objects spo shape-node (str sh "property"))
        parent-iris  (->> (objects spo shape-node (str sh "node"))
                          (filter iri?)
                          (map :value)
                          vec)]
    (when (iri? target-cls)
      {:node-iri          (when (iri? shape-node) (:value shape-node))
       :class-uri         (:value target-cls)
       :hidden?           (literal-truthy? mb-hide)
       :description       desc-text
       :parent-shape-iris parent-iris
       :properties        (vec (keep #(property-shape spo % lang) prop-nodes))})))

(defn- resolve-inheritance
  "For each shape that references parents via `sh:node`, recursively merge in
   the parents' properties. Child wins for any property that shares a path
   with a parent. Parent shapes that aren't themselves NodeShapes (e.g. when
   `sh:node` points at the target class IRI) are silently ignored. Cycles are
   broken with a visited set."
  [shapes]
  (let [by-iri (into {} (for [s shapes :when (:node-iri s)] [(:node-iri s) s]))]
    (letfn [(collect [visited node-iri]
              (if (or (visited node-iri) (not (by-iri node-iri)))
                {}
                (let [s        (by-iri node-iri)
                      visited' (conj visited node-iri)
                      parents  (apply merge
                                      (map #(collect visited' %) (:parent-shape-iris s)))
                      own      (into {} (map (juxt :property-uri identity) (:properties s)))]
                  (merge parents own))))]
      (mapv (fn [s]
              (assoc s :properties
                     (-> (collect #{} (:node-iri s)) vals vec)))
            shapes))))

(defn- merge-shapes-by-class
  "When multiple NodeShapes target the same class, merge their property lists
   (later wins per property URI)."
  [shapes]
  (->> shapes
       (group-by :class-uri)
       (map (fn [[class-uri ss]]
              {:class-uri   class-uri
               :hidden?     (boolean (some :hidden? ss))
               :description (some :description ss)
               :properties  (->> ss
                                 (mapcat :properties)
                                 (reduce (fn [acc p]
                                           (assoc acc (:property-uri p) p))
                                         {})
                                 vals)}))))

(defn shacl->metadata
  "Walk parsed `triples` and return a sequence of shape descriptors. Each is

       {:class-uri \"…\"
        :hidden? false
        :description \"…\" or nil
        :properties ({:property-uri \"…\"
                      :base-type :type/Text
                      :semantic-type :type/FK or nil
                      :description \"…\" or nil
                      :fk-target-class \"…\" or nil
                      :display-value-property \"…\" or nil
                      :database-required true|false
                      :lang-string? true|false
                      :hidden? false}
                     ...)}

   Shapes flagged `metabase:hide true` are pruned. Properties flagged
   `metabase:hide true` are pruned from their parent shape. `lang` (a BCP-47
   tag, may be blank) drives the language-preferred selection of
   `sh:name`/`sh:description` literals."
  [triples lang]
  (let [spo         (index-spo triples)
        ;; Pick up shapes via *either* `rdf:type sh:NodeShape` or by being the
        ;; subject of `sh:targetClass` — handles SHACL files that omit the type.
        shape-nodes (-> #{}
                        (into (keep (fn [[s preds]]
                                      (when (some (fn [o] (iri= o (str sh "NodeShape")))
                                                  (get preds (str rdf "type")))
                                        s))
                                    spo))
                        (into (keep (fn [[s preds]]
                                      (when (contains? preds (str sh "targetClass")) s))
                                    spo)))
        shapes      (->> shape-nodes
                         (keep #(shape spo % lang))
                         (remove :hidden?)
                         resolve-inheritance
                         merge-shapes-by-class)]
    (log/debugf "[shacl] Extracted %d shape(s) from %d triple(s) (lang=%s)"
                (count shapes) (count triples) (pr-str lang))
    (map (fn [s]
           (update s :properties (fn [ps] (remove :hidden? ps))))
         shapes)))

;; ----------------------------------------------------------------------------
;; Cache: avoid refetching/reparsing on every describe-table call within a sync.

(def ^:private cache (atom {}))
(def ^:private cache-ttl-ms (* 30 1000))

(defn- cache-lookup [k]
  (let [{:keys [at value]} (get @cache k)]
    (when (and at (< (- (System/currentTimeMillis) at) cache-ttl-ms))
      value)))

(defn- cache-store! [k value]
  (swap! cache assoc k {:at (System/currentTimeMillis) :value value})
  value)

(defn metadata
  "Return the cached SHACL metadata for `url` resolved under language `lang`.
   Fetches and parses if needed. `opts` is forwarded to [[fetch-shacl]] (HTTP
   timeouts and size cap); it does not affect the parsed result, so the cache
   key stays `[url lang]`. The cache key includes `lang` so switching language
   re-derives labels without forcing a fresh HTTP fetch on every call."
  ([url lang] (metadata url lang nil))
  ([url lang opts]
   (let [k [url lang]]
     (or (cache-lookup k)
         (cache-store! k
                       (-> (fetch-shacl url opts)
                           (parse-turtle url)
                           (shacl->metadata lang)))))))

(defn invalidate!
  "Forget the cached SHACL metadata for `url` (all languages)."
  [url]
  (swap! cache (fn [m] (into {} (remove (fn [[[u _] _]] (= u url)) m)))))
