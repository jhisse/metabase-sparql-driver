(ns metabase.driver.sparql.error-test
  "Unit tests for SPARQL connection error humanization.

  Metabase calls `humanize-connection-error-message` with the output of
  `metabase.util/all-ex-messages`, which is a sequence of strings collected
  from the exception cause chain. These tests pin that contract."
  (:require [clojure.test :refer :all]
            [metabase.driver.sparql.error :as error]))

(deftest humanize-connection-error-message-test
  (testing "matches HTTP 401 anywhere in the cause chain"
    (is (= :username-or-password-incorrect
           (error/humanize-connection-error-message
            ["SPARQL endpoint returned status: 401\nBody: Unauthorized"]))))

  (testing "matches across causes after the messages are joined"
    (is (= :username-or-password-incorrect
           (error/humanize-connection-error-message
            ["Connection failed: upstream error"
             "SPARQL endpoint returned status: 401"]))))

  (testing "returns the human string for 403"
    (is (= "Access denied: You do not have permission to access this endpoint (403 Forbidden)."
           (error/humanize-connection-error-message
            ["SPARQL endpoint returned status: 403"]))))

  (testing "returns the human string for 404"
    (is (= "Endpoint not found: Please check the SPARQL Endpoint URL (404 Not Found)."
           (error/humanize-connection-error-message
            ["SPARQL endpoint returned status: 404"]))))

  (testing "returns the human string for 500"
    (is (= "Server error: The SPARQL endpoint encountered an internal error (500 Internal Server Error)."
           (error/humanize-connection-error-message
            ["SPARQL endpoint returned status: 500"]))))

  (testing "matches Connection refused -> known keyword"
    (is (= :cannot-connect-check-host-and-port
           (error/humanize-connection-error-message
            ["java.net.ConnectException: Connection refused"]))))

  (testing "matches UnknownHostException -> :invalid-hostname"
    (is (= :invalid-hostname
           (error/humanize-connection-error-message
            ["java.net.UnknownHostException: no-such-host"]))))

  (testing "matches macOS getaddrinfo wording -> :invalid-hostname"
    (is (= :invalid-hostname
           (error/humanize-connection-error-message
            ["nodename nor servname provided, or not known"]))))

  (testing "unmatched input falls back to the outermost message"
    (is (= "Connection failed: Host URL cannot be nil"
           (error/humanize-connection-error-message
            ["Connection failed: Host URL cannot be nil"
             "Host URL cannot be nil"])))))
