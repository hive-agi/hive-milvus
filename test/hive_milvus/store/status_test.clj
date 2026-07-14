(ns hive-milvus.store.status-test
  (:require [clojure.test :refer [deftest is]]
            [hive-milvus.embed.port :as port]
            [hive-milvus.resilience.retry :as retry]
            [hive-milvus.store.entries :as entries]
            [hive-milvus.store.lookup :as lookup]
            [hive-test.mutation :as mut]
            [malli.core :as m]
            [milvus-clj.api :as milvus]))

(defn run-status [query-scalar]
  (with-redefs [retry/ensure-live! (fn [_] true)
                retry/with-auto-reconnect (fn [_ operation] (operation))
                lookup/known-collections (fn [_] ["memory_a" "memory_b"])
                milvus/connected? (constantly true)
                milvus/query-scalar query-scalar
                port/provider-available-for? (constantly true)]
    (entries/store-status (atom {}))))

(deftest status-uses-exact-count-aggregation
  (let [requests (atom [])
        status (run-status
                (fn [collection query]
                  (swap! requests conj [collection query])
                  (delay [(hash-map (keyword "count(*)")
                                    (if (= collection "memory_a") 26193 5666))])))]
    (is (m/validate entries/StoreStatus status))
    (is (= 31859 (:entry-count status)))
    (is (= {"memory_a" 26193 "memory_b" 5666} (:collections status)))
    (is (empty? (:errors status)))
    (is (every? #(= ["count(*)"] (get-in % [1 :output-fields])) @requests))
    (is (every? #(= 1 (get-in % [1 :limit])) @requests))))

(deftest failed-count-is-unavailable-not-zero
  (let [status (run-status
                (fn [collection _]
                  (if (= collection "memory_a")
                    (delay [(hash-map (keyword "count(*)") 7)])
                    (throw (ex-info "count failed" {:collection collection})))))]
    (is (m/validate entries/StoreStatus status))
    (is (nil? (:entry-count status)))
    (is (= {"memory_a" 7} (:collections status)))
    (is (= [{:collection "memory_b" :error "count failed"}]
           (:errors status)))))

(mut/deftest-mutation-witness false-zero-status-is-caught
  hive-milvus.store.entries/store-status
  (fn [_]
    {:backend "milvus"
     :configured? true
     :entry-count 0
     :collections {}
     :errors []
     :supports-search? true})
  (fn []
    (let [status (run-status
                  (fn [_ _]
                    (delay [(hash-map (keyword "count(*)") 26193)])))]
      (is (= 52386 (:entry-count status))))))
