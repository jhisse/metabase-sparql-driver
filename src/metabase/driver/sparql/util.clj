;; SPARQL Utilities for Metabase SPARQL Driver
;;
;; Este namespace fornece funções utilitárias para o driver SPARQL.
;; Inclui funções para extrair detalhes de conexão e outras operações auxiliares.
(ns metabase.driver.sparql.util
  (:require [clojure.tools.logging :as log]))

(defn extract-endpoint-details
  "Extrai os detalhes do endpoint SPARQL de um objeto de banco de dados ou consulta.
   
   Parâmetros:
     database-or-query - Objeto de banco de dados ou consulta
   Retorna:
     Mapa contendo :endpoint, :default-graph e :use-insecure?"
  [database-or-query]
  (let [details (or (:details database-or-query)
                     (get-in database-or-query [:database :details]))]
    {:endpoint (:endpoint details)
     :default-graph (:default-graph details)
     :insecure? (:use-insecure details)}))
