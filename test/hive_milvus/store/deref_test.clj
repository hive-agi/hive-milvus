(ns hive-milvus.store.deref-test
  "Contract tests for hive-milvus.store.deref/deref!."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.adt :as adt]
            [hive-milvus.failure :as failure]
            [hive-milvus.store.deref :as d]
            [hive-test.trifecta :as trifecta]))

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

(defn- bounded-outcome
  "Exercise the same operation labels the Milvus boundaries pass to deref!."
  [[op ready?]]
  (let [response (promise)]
    (when ready? (deliver response :ack))
    (try
      {:value (d/deref! op response 1)}
      (catch clojure.lang.ExceptionInfo e
        {:timeout? (:milvus/timeout (ex-data e))
         :op (:op (ex-data e))
         :timeout-ms (:timeout-ms (ex-data e))}))))

(trifecta/deftrifecta milvus-rpc-labels-have-bounded-outcomes
  bounded-outcome
  {:golden-path "test/golden/milvus/deref-outcomes.edn"
   :cases {:query-timeout [:query-scalar false]
           :write-timeout [:add false]
           :delete-timeout [:delete false]
           :index-timeout [:create-index false]
           :load-timeout [:load-collection false]
           :get-ready [:get true]}
   :pred (fn [outcome]
           (or (= :ack (:value outcome))
               (and (true? (:timeout? outcome))
                    (keyword? (:op outcome))
                    (= 1 (:timeout-ms outcome)))))
   :mutations [["unbounded-or-silent-timeout" (fn [_] {:value nil})]
               ["loses-operation-label" (fn [_] {:timeout? true :timeout-ms 1})]]})
