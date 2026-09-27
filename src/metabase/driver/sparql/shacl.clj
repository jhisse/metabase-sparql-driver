(ns metabase.driver.sparql.shacl
  "SHACL-based metadata sync for the SPARQL driver.

   Given a URL serving a SHACL document in Turtle, this namespace:
   - fetches and parses the document,
   - extracts shapes (one per RDF class / Metabase table),
   - extracts property shapes (one per Metabase column),
   - flattens `sh:node` inheritance,
   - resolves foreign-key relationships declared via `sh:class`,
   - honors a small `metabase:` vocabulary for sync-time overrides
     (`hide`, `semanticType`, `displayValueProperty`).

   The public entry point is [[metadata]] which returns a fully-resolved
   intermediate description that [[metabase.driver.sparql.database]] turns
   into the maps Metabase's sync interface expects. Results are cached per
   URL, language and fetch options for 30 seconds so a single sync run only
   fetches once."
  (:require [clj-http.client :as http]
            [clojure.string :as str]
            [metabase.driver.sparql.uri :as uri]
            [metabase.util.log :as log])
  (:import (java.io ByteArrayOutputStream InputStream StringReader)
           (org.apache.http.client.methods HttpUriRequest)
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
  "Convert the RDF4J value `v` into a map tagged by `:type`:
   `{:type :iri :value …}`, `{:type :bnode :id …}`,
   `{:type :literal :value … :datatype … :lang …}` (`:lang` nil when untagged)
   or `{:type :unknown :value …}`."
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

(defn- too-large
  "Return the error for a SHACL document from `shown-url` of `bytes` bytes or
   more, over the `max-bytes` limit."
  [shown-url bytes max-bytes]
  (ex-info (format "SHACL document from %s exceeds the %d-byte limit (%d bytes or more)"
                   shown-url max-bytes bytes)
           {:url shown-url :bytes bytes :max-bytes max-bytes}))

(defn- read-capped
  "Read `in` into a UTF-8 string. Throws as soon as more than `max-bytes` bytes
   have arrived, or once the clock passes `deadline` (epoch millis), so a huge
   or trickling document is not downloaded to the end."
  [^InputStream in max-bytes deadline shown-url]
  (let [out (ByteArrayOutputStream.)
        buf (byte-array 8192)]
    (loop []
      (let [n (.read in buf)]
        (when-not (neg? n)
          (.write out buf 0 n)
          (when (> (.size out) max-bytes)
            (throw (too-large shown-url (.size out) max-bytes)))
          (when (> (System/currentTimeMillis) deadline)
            (throw (ex-info (format "SHACL document from %s took too long to download" shown-url)
                            {:url shown-url})))
          (recur))))
    (.toString out "UTF-8")))

(defn- release!
  "Release the `:as :stream` response `resp` without reading the rest of its
   body. clj-http's close drains the body first, which would download a huge
   or endless document to the end, so the request is aborted before."
  [resp]
  (some-> ^HttpUriRequest (get-in resp [:request :http-req]) .abort)
  (some-> ^InputStream (:body resp) .close))

(defn fetch-shacl
  "Return the body of the SHACL document at `url` as a string, requested as
   `text/turtle`.

   `opts` may supply `:connect-timeout-ms`, `:socket-timeout-ms` and
   `:max-bytes`; each falls back to a built-in default (10 s, 30 s, 10 MB).
   The socket timeout bounds each read and, counted once the response
   arrives, the whole body, which can therefore take up to about twice that.
   The size cap is checked against `Content-Length` and while reading. A
   body that is not read to the end is aborted, not drained.

   Redirects are followed, unless `url` carries credentials: the HTTP client
   would re-send them to the new host.

   Throws an `ex-info` on any non-200 response or when the body exceeds the
   size cap; connection errors and timeouts propagate from clj-http."
  ([url] (fetch-shacl url nil))
  ([url {:keys [connect-timeout-ms socket-timeout-ms max-bytes]}]
   (let [connect-ms (or connect-timeout-ms default-connect-timeout-ms)
         socket-ms  (or socket-timeout-ms default-socket-timeout-ms)
         max-bytes  (or max-bytes default-max-bytes)
         shown-url  (uri/redact-userinfo url)]
     (log/infof "[shacl] Fetching SHACL document from %s" shown-url)
     (let [resp (http/get url (cond-> {:headers            {"Accept" "text/turtle"}
                                       :throw-exceptions   false
                                       :connection-timeout connect-ms
                                       :socket-timeout     socket-ms
                                       :as                 :stream
                                       ;; Keeps :http-req in the response, for release!
                                       :save-request       true}
                                (not= url shown-url) (assoc :redirect-strategy :none)))]
       (try
         (if (= 200 (:status resp))
           (do
             (when-let [length (some-> (get-in resp [:headers "content-length"]) str str/trim parse-long)]
               (when (> length max-bytes)
                 (throw (too-large shown-url length max-bytes))))
             (read-capped (:body resp) max-bytes (+ (System/currentTimeMillis) socket-ms) shown-url))
           (throw (ex-info (format "Failed to fetch SHACL document from %s (status %s)"
                                   shown-url (:status resp))
                           {:url shown-url :status (:status resp)})))
         (finally
           (release! resp)))))))

(def ^:private no-contexts
  "Empty array for the trailing `Resource...` varargs on `Rio/parse`. Clojure's
   reflector doesn't synthesize an empty varargs array, so we materialize one."
  (make-array Resource 0))

(defn parse-turtle
  "Parse Turtle `text` into a vector of `[s p o]` triples, where each term is a
   small map (see [[term]]). Relative IRIs resolve against `base-iri` (nil
   means none). Throws the RDF4J parse exception on malformed Turtle."
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
  "Return all objects for `subject`/`pred-iri`, or nil if none."
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
  "Return the semantic-type keyword named by the literal or IRI `t`:
   `\"type/URL\"` and `\":type/URL\"` both give `:type/URL`, and any other
   non-blank value is keywordized as-is. nil when `t` is nil, blank or a
   blank node."
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
  "Return the string value of the literal in `terms` that best fits `lang`:
   one tagged `lang`, else an untagged one, else the first literal. nil when
   `terms` holds no literal."
  [terms lang]
  (let [lits          (filter literal? terms)
        match-lang    (when-not (str/blank? lang)
                        (first (filter #(= lang (:lang %)) lits)))
        untagged      (first (filter #(str/blank? (:lang %)) lits))
        any           (first lits)]
    (some-> (or match-lang untagged any) :value)))

(defn- property-shape
  "Return the property descriptor (see [[shacl->metadata]]) of the
   property-shape node `prop-node`, or nil when its `sh:path` is not a single
   IRI (complex paths are not synced). `lang` (may be nil/blank) picks the
   `sh:name` / `sh:description` literals, which are joined into
   `:description`."
  [spo prop-node lang]
  (let [path        (single spo prop-node (str sh "path"))
        datatype    (single spo prop-node (str sh "datatype"))
        target-cls  (single spo prop-node (str sh "class"))
        node-kind   (single spo prop-node (str sh "nodeKind"))
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
        descr       (->> [name-text desc-text]
                         (remove str/blank?)
                         (str/join " — "))]
    (when (iri? path)
      {:property-uri      (:value path)
       :base-type         base-type
       :semantic-type     sem-type
       :description       (when-not (str/blank? descr) descr)
       :order             (parse-long-literal order-lit)
       ;; Values are IRI nodes: an explicit sh:nodeKind sh:IRI, or an sh:class
       ;; target (FK). Synced as :database-type "uri" (see shacl-prop->field).
       :iri-kind?         (boolean (or (iri? target-cls)
                                       (and (iri? node-kind)
                                            (= (:value node-kind) (str sh "IRI")))))
       :fk-target-class   (when (iri? target-cls) (:value target-cls))
       :display-value-property (when (iri? mb-display) (:value mb-display))
       :database-required (boolean (some-> (parse-long-literal min-count) pos?))
       :lang-string?      lang-string?
       :hidden?           (literal-truthy? mb-hide)})))

(defn- shape
  "Return the shape descriptor of `shape-node`, or nil for a shape without an
   IRI `sh:targetClass` (only class-targeted shapes are synced).

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
  "Return `shapes` with each shape's properties merged recursively with those
   of the parents it references via `sh:node`. Child wins for any property
   that shares a path with a parent. A parent that is not itself one of
   `shapes` (no `sh:targetClass`, flagged `metabase:hide`, or an `sh:node`
   pointing at a class IRI instead of a shape) is silently ignored. Cycles
   are broken with a visited set."
  [shapes]
  (let [by-iri (into {} (for [s shapes :when (:node-iri s)] [(:node-iri s) s]))]
    (letfn [(collect-shape [visited s]
              (let [parents (apply merge
                                   (map #(collect visited %) (:parent-shape-iris s)))
                    own     (into {} (map (juxt :property-uri identity) (:properties s)))]
                (merge parents own)))
            (collect [visited node-iri]
              (if (or (visited node-iri) (not (by-iri node-iri)))
                {}
                (collect-shape (conj visited node-iri) (by-iri node-iri))))]
      ;; Start from the shape itself, not its IRI: a blank-node shape has no
      ;; :node-iri and would otherwise lose all of its properties.
      (mapv (fn [s]
              (assoc s :properties
                     (-> (collect-shape (cond-> #{} (:node-iri s) (conj (:node-iri s))) s)
                         vals vec)))
            shapes))))

(defn- merge-shapes-by-class
  "Merge the shapes that target the same class into one, with the union of
   their properties (on a property-URI conflict an arbitrary one wins, since
   input order comes from a hash set)."
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
                      :semantic-type keyword (e.g. :type/FK, :type/URL) or nil
                      :description \"…\" or nil
                      :fk-target-class \"…\" or nil
                      :display-value-property \"…\" or nil
                      :order long or nil
                      :iri-kind? true|false
                      :database-required true|false
                      :lang-string? true|false
                      :hidden? false}
                     ...)}

   Shapes flagged `metabase:hide true` are pruned. Properties flagged
   `metabase:hide true` are pruned from their parent shape. Properties
   inherited via `sh:node` are flattened in, and shapes targeting the same
   class are merged into one. `lang` (a BCP-47
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
  "Return the [[shacl->metadata]] shapes of the SHACL document at `url` under
   language `lang`, fetching and parsing it unless `[url lang opts]` was
   cached in the last 30 seconds. `opts` is forwarded to [[fetch-shacl]] (HTTP
   timeouts and size cap) and is part of the cache key, so a database never
   gets a document fetched under another database's size cap. Fetch and parse
   errors throw and are not cached."
  ([url lang] (metadata url lang nil))
  ([url lang opts]
   (let [k [url lang opts]]
     (or (cache-lookup k)
         (cache-store! k
                       (-> (fetch-shacl url opts)
                           (parse-turtle url)
                           (shacl->metadata lang)))))))

(defn invalidate!
  "Forget the cached SHACL metadata for `url` (all languages and options)."
  [url]
  (swap! cache (fn [m] (into {} (remove (fn [[[u] _]] (= u url)) m)))))
