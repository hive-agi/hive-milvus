(ns hive-milvus.store.deref-test
  "Contract tests for hive-milvus.store.deref/deref!."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.adt :as adt]
            [hive-milvus.failure :as failure]
            [hive-milvus.store.deref :as d]))

(deftest deref-returns-value-when-ready
  (testing "a delivered future derefs to its value"
    (is (= [:row] (d/deref! :get (doto (promise) (deliver [:row])) 1000)))))

(deftest deref-throws-tagged-timeout
  (testing "a never-delivered future throws a :milvus/timeout-tagged ex-info"
    (let [never (promise)
          e     (try (d/deref! :get never 50) nil
                     (catch clojure.lang.ExceptionInfo ex ex))]
      (is (some? e) "must throw on timeout, not hang or return nil")
      (is (true? (:milvus/timeout (ex-data e))))
      (is (= :get (:op (ex-data e)))))))

(deftest timeout-classifies-transient
  (testing "timeout ex-info classifies as :milvus/transient"
    (let [never (promise)
          e     (try (d/deref! :get never 50)
                     (catch clojure.lang.ExceptionInfo ex ex))
          f     (failure/classify e)]
      (is (= :milvus/transient (adt/adt-variant f)))
      (is (failure/transient? f)))))

(deftest timeout-ms-default
  (testing "default budget is 5000ms when env unset"
    (when-not (System/getenv "MILVUS_DEREF_TIMEOUT_MS")
      (is (= 5000 (d/timeout-ms))))))
