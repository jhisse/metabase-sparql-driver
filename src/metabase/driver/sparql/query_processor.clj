;; SPARQL Query Processor for Metabase SPARQL Driver
;;
;; Este namespace lida com o processamento de consultas SPARQL e transformação de resultados.
;; Fornece funções para extrair metadados e converter resultados para o formato esperado pelo Metabase.
(ns metabase.driver.sparql.query-processor
  (:require [metabase.driver.sparql.conversion :as conversion]))

(defn process-query-results
  "Processa os resultados de uma consulta SPARQL.
   
   Parâmetros:
     result - Resultado da consulta SPARQL em formato JSON
     respond - Função para chamar com os metadados e linhas
   
   Retorna:
     Resultado da chamada à função respond"
  [result respond]
  (let [;; Extrai nomes de variáveis do cabeçalho da resposta
        vars (get-in result [:head :vars])

        ;; Extrai bindings da resposta
        bindings (get-in result [:results :bindings])
        first-row (first bindings)

        ;; Determina tipos de colunas baseado na primeira linha
        col-types (reduce (fn [types var-name]
                            (let [binding (get first-row (keyword var-name))]
                              (assoc types var-name
                                     (if binding
                                       (conversion/sparql-type->base-type (:type binding) (:datatype binding))
                                       :type/Text))))
                          {}
                          vars)

        ;; Cria metadados para o conjunto de resultados
        metadata {:cols (map (fn [var-name]
                               {:name var-name
                                :display_name var-name
                                :base_type (get col-types var-name :type/Text)})
                             vars)}

        ;; Transforma os bindings da resposta SPARQL JSON em linhas com valores convertidos
        rows (map (fn [binding]
                    ;; Converte cada binding para um vetor de valores na mesma ordem que vars
                    (mapv (fn [var-name]
                            (let [var-binding (get binding (keyword var-name))]
                              (if var-binding
                                (conversion/convert-value var-binding)
                                nil)))
                          vars))
                  bindings)]

    ;; Chama a função respond com metadados e linhas
    (respond metadata rows)))
