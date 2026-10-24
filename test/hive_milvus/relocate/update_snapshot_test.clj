(ns hive-milvus.relocate.update-snapshot-test
  (:require [clojure.test :refer [deftest is]]
            [hive-dsl.result :as r]
            [hive-milvus.relocate.collectors.entry :as collector]
            [hive-milvus.relocate.boundary :as boundary]))

(def entry {:id "e1" :type :note :content "same" :tags ["a"]})
(def coll "hive_mcp_memory_4d")
(def bundle {:id "e1" :src-coll coll :target-coll coll
             :entry entry :existing entry})

(deftest one-snapshot-keeps-the-vector-outside-the-entry
  (let [calls (atom [])
        fetch (fn [c id]
                (swap! calls conj [c id])
                (when (= c coll)
                  {:entry entry :stored-embedding [1 2 3 4]}))
        res (collector/collect-update-with fetch ["empty" coll "unvisited"] {:id "e1"})]
    (is (r/ok? res))
    (is (= [["empty" "e1"] [coll "e1"]] @calls))
    (is (= entry (:entry (:ok res))))
    (is (= [1 2 3 4] (:stored-embedding (:ok res))))
    (is (not (contains? (:entry (:ok res)) :embedding)))))

(deftest snapshot-collection-retains-absence-and-outage-semantics
  (is (= :collector/not-found
         (:error (collector/collect-update-with (constantly nil) [coll] {:id "e1"}))))
  (is (= :milvus/read-incomplete
         (:error (collector/collect-update-with
                   (fn [_ _] (throw (ex-info "transport down" {})))
                   [coll] {:id "e1"})))))

(deftest valid-snapshot-never-issues-a-second-read
  (let [calls (atom 0)
        read (fn [_ _] (swap! calls inc) (r/ok [9 9 9 9]))
        res (boundary/keep-vector read (assoc bundle :stored-embedding [1 2 3 4]))]
    (is (= [1 2 3 4] (:kept-embedding res)))
    (is (zero? @calls))))

(deftest invalid-snapshot-falls-back-without-mixing-snapshots
  (doseq [v [nil [] [1 2]]]
    (let [calls (atom 0)
          read (fn [_ _] (swap! calls inc) (r/ok [9 9 9 9]))
          res (boundary/keep-vector read (assoc bundle :stored-embedding v))]
      (is (nil? (:kept-embedding res)))
      (is (zero? @calls)))))

(deftest changed-text-or-target-does-not-reuse-snapshot
  (doseq [b [(assoc-in bundle [:entry :content] "changed")
             (assoc bundle :target-coll "scratch_8d")]]
    (is (nil? (:kept-embedding
                (boundary/keep-vector (fn [_ _] (throw (Exception. "must not read")))
                                      (assoc b :stored-embedding [1 2 3 4])))))))

(deftest snapshot-hit-wins-over-a-failing-neighbour
  (let [res (collector/collect-update-with
              (fn [c _]
                (if (= c coll)
                  {:entry entry :stored-embedding [1 2 3 4]}
                  (throw (ex-info "unreachable" {}))))
              ["broken" coll] {:id "e1"})]
    (is (r/ok? res))
    (is (= coll (:src-coll (:ok res))))))
