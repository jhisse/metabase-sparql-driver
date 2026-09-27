(ns metabase.driver.sparql.error
  "Translate connection failures into the messages Metabase shows on the
   database connection form."
  (:require [clojure.string :as str]
            [metabase.util.log :as log]))

(defn humanize-connection-error-message
  "Turn the exception chain's `messages` (from `metabase.util/all-ex-messages`)
   into a readable message or one of Metabase's known error keywords.

   Matches HTTP statuses 401, 403, 404, 500, 502 and 503, refused connections
   and unknown hosts anywhere in the chain; anything else returns the first
   message unchanged."
  [messages]
  (log/debugf "[humanize-connection-error-message] - Received messages: %s" messages)
  (let [joined (str/join " -> " messages)]
    (condp re-matches joined
      #"(?s).*status: 401.*"
      :username-or-password-incorrect

      #"(?s).*status: 403.*"
      "Access denied: You do not have permission to access this endpoint (403 Forbidden)."

      #"(?s).*status: 404.*"
      "Endpoint not found: Please check the SPARQL Endpoint URL (404 Not Found)."

      #"(?s).*status: 500.*"
      "Server error: The SPARQL endpoint encountered an internal error (500 Internal Server Error)."

      #"(?s).*status: 502.*"
      "Bad gateway: The server received an invalid response from an upstream server (502 Bad Gateway)."

      #"(?s).*status: 503.*"
      "Service unavailable: The SPARQL endpoint is currently unavailable (503 Service Unavailable)."

      #"(?s).*Connection refused.*" :cannot-connect-check-host-and-port

      #"(?s).*UnknownHostException.*"
      :invalid-hostname

      #"(?s).*nodename nor servname provided.*"
      :invalid-hostname

      (first messages))))
