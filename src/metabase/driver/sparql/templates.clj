;; SPARQL Queries for Metabase SPARQL Driver
;;
;; Este namespace contém consultas SPARQL predefinidas usadas pelo driver
;; para várias operações como teste de conexão e descoberta de tabelas.
;; Cada consulta é otimizada para casos de uso específicos.
(ns metabase.driver.sparql.templates)

;; Consulta simples para testar conectividade com um endpoint SPARQL
;;
;; Retorna:
;;   Uma única linha com uma única coluna chamada ?ping contendo o valor 'pong'
;;
;; Uso:
;;   Usada pelo método driver/can-connect? para verificar a disponibilidade do endpoint
;;   Uma execução bem-sucedida desta consulta indica que o endpoint está acessível
(defn connection-test-query []
  "SELECT ?ping 
   WHERE { 
     BIND('pong' AS ?ping) 
   }")

;; Consulta para descobrir classes RDF no endpoint
;;
;; Retorna:
;;   Uma lista de classes RDF (?class) com suas contagens de instâncias (?count)
;;   Limitada às 100 principais classes por contagem de instâncias
;;
;; Uso:
;;   Usada pelo método driver/describe-database para descobrir "tabelas" disponíveis
;;   Cada classe RDF é tratada como uma tabela no modelo de dados do Metabase
;;   A contagem ajuda a identificar as classes mais significativas no conjunto de dados
(defn classes-discovery-query []
  "SELECT ?class (COUNT(?s) AS ?count) 
   WHERE { 
     ?s a ?class 
   } 
   GROUP BY ?class 
   ORDER BY DESC(?count) 
   LIMIT 100")
