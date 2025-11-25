(ns metabase.driver.sparql.error
  "Error handling for Metabase SPARQL Driver."
  (:require [metabase.util.log :as log]))

(defn humanize-connection-error-message
  "Humanizes the connection error message.
   
   Parameters:
     message - The raw error message string.
     
   Returns:
     A user-friendly error message string."
  [message]
  (log/debugf "[humanize-connection-error-message] - Received message: %s" message)
  (condp re-matches message
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

    message))
