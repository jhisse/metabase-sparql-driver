(ns metabase.driver.sparql.properties
  "Connection properties for the SPARQL driver."
  (:require [metabase.driver.common :as driver.common]
            [metabase.util :as u]))

(def connection-properties
  "Connection properties definition for SPARQL driver."
  (->>
   [{:name         "endpoint"
     :display-name "SPARQL Endpoint"
     :placeholder  "http://localhost:3030/ds/query"
     :required     true}
    {:name         "use-insecure"
     :display-name "Ignore SSL Certificate Errors"
     :type         :boolean
     :default      false}
    {:name         "default-graph"
     :display-name "Default Graph URI"
     :placeholder  "http://example.org/graph"}
    driver.common/advanced-options-start
    {:name         "metadata-sync-strategy"
     :display-name "Metadata Sync Strategy"
     :type         :select
     :default      "auto"
     :options      [{:name  "Automatic (Discover from Endpoint)"
                     :value "auto"}
                    {:name  "None (Don't sync)"
                     :value "none"}
                    {:name  "Explicit (Use JSON Configuration)"
                     :value "explicit"}]
     :description  "Choose how Metabase discovers tables and fields."
     :visible-if   {:advanced-options true}}
    {:name         "schema-config"
     :display-name "Schema Configuration (JSON)"
     :type         :text
     :placeholder  (str "{\n"
                        "  \"tables\": [\n"
                        "    {\n"
                        "      \"name\": \"http://example.org/Person\",\n"
                        "      \"fields\": [\n"
                        "        \"http://xmlns.com/foaf/0.1/givenName\",\n"
                        "        \"http://xmlns.com/foaf/0.1/familyName\"\n"
                        "      ]\n"
                        "    }\n"
                        "  ]\n"
                        "}")
     :description  "Explicitly define tables and fields to skip discovery. Format: JSON object with a tables array."
     :visible-if   {:metadata-sync-strategy "explicit"}}]
   (into [] (mapcat u/one-or-many))))
