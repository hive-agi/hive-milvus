(ns hive-milvus.store.locate-collection-test
  "Trifecta for `lookup/locate-collection`, the outcome-honest fold the
   relocate collector uses to find an entry's collection.

   No Milvus: FETCH is a stub port built from a per-collection behaviour
   (:hit / :miss / :fail / :missing), so the fold is exercised as data."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.generators :as gen]
            [hive-dsl.result :as r]
            [hive-test.trifecta :refer [deftrifecta]]
            [hive-milvus.store.lookup :as lookup]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(defn- stub-fetch
  "FETCH port from `behaviour`, a map coll -> :hit | :miss | :fail | :missing."
  [behaviour]
  (fn [coll id]
    (case (get behaviour coll :miss)
      :hit     {:id id :coll coll}
      :miss    nil
      :fail    (throw (ex-info "UNAVAILABLE: connection refused" {}))
      :missing (throw (ex-info "collection not found[collection=x]" {})))))

(defn- args
  "Subject arguments for an ordered seq of [coll behaviour] pairs."
  [pairs]
  [(stub-fetch (into {} pairs)) (mapv first pairs) "id-1"])

(defn- expected
  "Reference outcome for `pairs`, computed from the behaviours alone."
  [pairs]
  (let [hit    (some (fn [[c b]] (when (= :hit b) c)) pairs)
        before (if hit (take-while (fn [[c _]] (not= c hit)) pairs) pairs)
        failed (filterv (fn [[_ b]] (= :fail b)) before)]
    (cond
      hit          [:hit hit]
      (seq failed) [:incomplete (mapv first failed)]
      :else        [:absent])))

(defn- outcome
  "Shape of a `locate-collection` result, comparable to `expected`."
  [res]
  (cond
    (and (r/ok? res) (:ok res)) [:hit (:collection (:ok res))]
    (r/ok? res)                 [:absent]
    :else                       [:incomplete (mapv :collection (:failed res))]))

(def gen-pairs
  (gen/fmap (fn [bs] (mapv (fn [i b] [(str "c" i) b]) (range) bs))
            (gen/vector (gen/elements [:hit :miss :fail :missing]) 0 6)))

(deftrifecta locate-collection
  hive-milvus.store.lookup/locate-collection
  {:golden-path "test/golden/milvus/locate-collection.edn"
   :apply?      true
   :cases       {:hit-first      (args [["a" :hit] ["b" :fail]])
                 :hit-after-fail (args [["a" :fail] ["b" :hit]])
                 :all-miss       (args [["a" :miss] ["b" :missing]])
                 :outage         (args [["a" :miss] ["b" :fail]])
                 :no-collections (args [])}
   :xf          outcome
   :gen         (gen/fmap args gen-pairs)
   :pred        (fn [res] (contains? #{:hit :absent :incomplete} (first (outcome res))))
   :num-tests   200
   :mutations   [["outage-as-absence"
                  ;; The old find-entry-collection: every failure reads as nil.
                  (fn [fetch colls id]
                    (r/ok (some (fn [c]
                                  (when-some [v (try (fetch c id) (catch Exception _ nil))]
                                    {:collection c :value v}))
                                colls)))]
                 ["missing-is-outage"
                  ;; A never-created collection counted as a failure.
                  (fn [fetch colls id]
                    (let [failed (atom [])
                          hit    (some (fn [c]
                                         (try (some->> (fetch c id) (hash-map :collection c :value))
                                              (catch Exception e
                                                (swap! failed conj {:collection c :message (ex-message e)})
                                                nil)))
                                       colls)]
                      (cond hit            (r/ok hit)
                            (seq @failed)  (r/err :milvus/read-incomplete {:id id :failed @failed})
                            :else          (r/ok nil))))]]})

(deftest locate-collection-matches-reference
  (testing "every behaviour mix answers what the reference model predicts"
    (doseq [pairs (gen/sample gen-pairs 300)]
      (is (= (expected pairs)
             (outcome (apply lookup/locate-collection (args pairs))))
          (pr-str pairs)))))
