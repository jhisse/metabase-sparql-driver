(ns metabase.driver.sparql.auth-test
  (:require [clojure.test :refer :all]
            [metabase.driver.sparql.auth :as auth]))

(deftest http-options-default
  (testing "missing/blank/none/unknown auth-type yields an empty fragment"
    (is (= {} (auth/http-options {})))
    (is (= {} (auth/http-options {:auth-type nil})))
    (is (= {} (auth/http-options {:auth-type ""})))
    (is (= {} (auth/http-options {:auth-type "none"})))
    (is (= {} (auth/http-options {:auth-type "NONE"})))
    (is (= {} (auth/http-options {:auth-type "  none  "})))
    (is (= {} (auth/http-options {:auth-type "weird"})))))

(deftest http-options-basic
  (testing "basic auth"
    (is (= {:basic-auth ["alice" "secret"]}
           (auth/http-options {:auth-type "basic"
                               :auth-username "alice"
                               :auth-password "secret"}))))
  (testing "basic is case-insensitive and trims"
    (is (= {:basic-auth ["alice" "secret"]}
           (auth/http-options {:auth-type "  Basic "
                               :auth-username "alice"
                               :auth-password "secret"}))))
  (testing "blank credentials drop the fragment so we don't send `Basic Og==`"
    (is (= {} (auth/http-options {:auth-type "basic"
                                  :auth-username ""
                                  :auth-password "secret"})))
    (is (= {} (auth/http-options {:auth-type "basic"
                                  :auth-username "alice"
                                  :auth-password ""})))
    (is (= {} (auth/http-options {:auth-type "basic"})))))

(deftest http-options-bearer
  (testing "bearer produces an Authorization header"
    (is (= {:headers {"Authorization" "Bearer eyJ.token"}}
           (auth/http-options {:auth-type "bearer"
                               :auth-bearer-token "eyJ.token"}))))
  (testing "bearer is case-insensitive and trims"
    (is (= {:headers {"Authorization" "Bearer eyJ.token"}}
           (auth/http-options {:auth-type "BEARER"
                               :auth-bearer-token "eyJ.token"}))))
  (testing "blank token drops the fragment"
    (is (= {} (auth/http-options {:auth-type "bearer"
                                  :auth-bearer-token ""})))
    (is (= {} (auth/http-options {:auth-type "bearer"
                                  :auth-bearer-token "   "})))
    (is (= {} (auth/http-options {:auth-type "bearer"
                                  :auth-bearer-token nil})))))
