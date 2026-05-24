(ns metabase.driver.sparql.auth
  "Authentication helpers for HTTP requests to the SPARQL endpoint.

   Reads the auth-related connection-properties off a database `details` map
   and returns a clj-http option fragment the caller can merge into its
   request. Returns `{}` for the `none` mode (or when required fields are
   blank), so callers can apply the result unconditionally."
  (:require [clojure.string :as str]))

(defn- normalize-type
  [auth-type]
  (some-> auth-type str str/trim str/lower-case))

(defn http-options
  "Build a clj-http option fragment from the auth-related fields on a database
   `details` map.

   Recognised keys on `details`:
     :auth-type          – \"none\" (default), \"basic\", or \"bearer\".
     :auth-username      – username for basic auth.
     :auth-password      – password for basic auth.
     :auth-bearer-token  – token sent as `Authorization: Bearer <token>`.

   Returns:
     {:basic-auth [user pass]}                   for basic
     {:headers {\"Authorization\" \"Bearer …\"}} for bearer
     {}                                          otherwise"
  [{:keys [auth-type auth-username auth-password auth-bearer-token]}]
  (case (normalize-type auth-type)
    "basic"
    (if (and (not (str/blank? auth-username))
             (not (str/blank? auth-password)))
      {:basic-auth [auth-username auth-password]}
      {})

    "bearer"
    (if-not (str/blank? auth-bearer-token)
      {:headers {"Authorization" (str "Bearer " auth-bearer-token)}}
      {})

    {}))
