(ns hive-milvus.auth-test
  (:require [clojure.test :refer [deftest testing is]]
            [hive-milvus.auth :as auth]))

(def ^:private fake-pass
  {"milvus/root-password" "s3cret"})

(defn- pass-fn [path] (get fake-pass path))

(deftest resolve-secret-test
  (testing "plain string passes through"
    (is (= "abc" (auth/resolve-secret "abc" pass-fn))))
  (testing "pass: ref resolves through password-store"
    (is (= "s3cret" (auth/resolve-secret "pass:milvus/root-password" pass-fn))))
  (testing "missing pass entry, blank and non-string resolve to nil"
    (is (nil? (auth/resolve-secret "pass:nope" pass-fn)))
    (is (nil? (auth/resolve-secret "  " pass-fn)))
    (is (nil? (auth/resolve-secret nil pass-fn)))))

(deftest effective-token-test
  (testing "explicit token wins over user/password"
    (is (= "tok" (auth/effective-token {:token "tok" :user "root" :password "x"} pass-fn))))
  (testing "user + password compose basic-auth bearer"
    (is (= "root:s3cret"
           (auth/effective-token {:user "root" :password "pass:milvus/root-password"} pass-fn))))
  (testing "user without password yields no token"
    (is (nil? (auth/effective-token {:user "root"} pass-fn))))
  (testing "no credentials yields no token"
    (is (nil? (auth/effective-token {} pass-fn)))))

(deftest with-effective-token-test
  (testing "sets :token and drops raw :password"
    (let [out (auth/with-effective-token {:host "h" :user "root" :password "pass:milvus/root-password"} pass-fn)]
      (is (= "root:s3cret" (:token out)))
      (is (not (contains? out :password)))
      (is (= "h" (:host out)))))
  (testing "no credentials leaves :token absent"
    (is (not (contains? (auth/with-effective-token {:host "h"} pass-fn) :token)))))
