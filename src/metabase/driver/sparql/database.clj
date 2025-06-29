;; SPARQL Database for Metabase SPARQL Driver
;;
;; This namespace handles the discovery and description of "tables" (RDF classes)
;; in SPARQL endpoints for Metabase.
(ns metabase.driver.sparql.database
  (:require [clojure.tools.logging :as log]
            [clojure.string :as str]
            [metabase.driver.sparql.execute :as execute]
            [metabase.driver.sparql.templates :as templates]))

(defn- extract-class-name
  "Extracts the class name from a URI.
   
   Parameters:
     class-uri - RDF class URI
   
   Returns:
     Class name extracted from the last part of the URI (after the last '/' or '#')."
  [class-uri]
  (let [last-part (last (or (re-seq #"[^/#]+$" class-uri)
                            (re-seq #"[^/]+$" class-uri)
                            (re-seq #"[^#]+$" class-uri)
                            [class-uri]))]
    (if (str/blank? last-part)
      class-uri
      last-part)))

(defn describe-table
  "Descreve os campos (propriedades) de uma classe RDF (tabela SPARQL)."
  [_ database table]
  (let [endpoint (-> database :details :endpoint)
        options {:insecure? (-> database :details :use-insecure)
                 :default-graph (-> database :details :default-graph)}
        class-uri (:name table)
        query (str "SELECT DISTINCT ?property ?type WHERE { "
                   "?instance a <" class-uri "> ; "
                   "?property ?value . "
                   "OPTIONAL { ?value a ?type } } LIMIT 100")
        [success result] (execute/execute-sparql-query endpoint query options)]
    (if success
      (let [bindings (get-in result [:results :bindings])
            fields (set
                    (for [binding bindings
                          :let [property-uri (get-in binding [:property :value])
                                type-uri (get-in binding [:type :value])]]
                      {:name          property-uri
                       :database-type "string"
                       :base-type     "string"
                       :pk?           false}))]
        ;; Adiciona o campo do identificador do recurso como PK
        {:fields (conj fields
                       {:name          "id"
                        :database-type "uri"
                        :base-type     "string"
                        :pk?           true})})
      (do
        (log/error "Error describing SPARQL table:" result)
        {:fields #{}}))))

(defn describe-database
  "Discovers the available 'tables' (RDF classes) in the SPARQL endpoint.

   Parameters:
     _ - driver (não utilizado)
     database - instância do Database do Metabase

   Returns:
     Map com a chave :tables contendo um conjunto de definições de tabelas."
  [_ database]
  (let [endpoint (-> database :details :endpoint)
        options {:insecure? (-> database :details :use-insecure)
                 :default-graph (-> database :details :default-graph)}
        [success result] (execute/execute-sparql-query endpoint (templates/classes-discovery-query 10) options)]
    (if success
      (let [classes-with-counts (map (fn [binding]
                                       {:uri (get-in binding [:class :value])
                                        :count (Integer/parseInt (get-in binding [:count :value]))})
                                     (get-in result [:results :bindings]))]
        {:tables
         (set
          (for [{:keys [uri count]} classes-with-counts]
            {:name uri
             :schema nil
             :display-name (extract-class-name uri)
             :description (str "RDF Class: " uri " (Instances: " count ")")}))})
      (do
        (log/error "Error describing SPARQL database:" result)
        {:tables #{}}))))