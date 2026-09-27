(ns metabase.driver.sparql.auth
  "Build the clj-http authentication options (basic or bearer) for requests to
   the SPARQL endpoint from a database's connection details."
  (:require [clojure.string :as str]))

(defn- normalize-type
  [auth-type]
  (some-> auth-type str str/trim str/lower-case))

(defn http-options
  "Return the clj-http options that authenticate a request, from the auth
   fields of a database `details` map.

   `:auth-type` (case- and whitespace-insensitive) \"basic\" gives
   `{:basic-auth [user pass]}` and \"bearer\" gives an
   `Authorization: Bearer <token>` header. \"none\", an unknown type, or a
   blank required field gives `{}`, so callers can merge the result
   unconditionally."
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
