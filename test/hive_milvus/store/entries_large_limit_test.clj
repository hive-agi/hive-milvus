(ns hive-milvus.store.entries-large-limit-test
  "A scalar query whose :limit exceeds Milvus's per-query cap.

   The FETCH port is a stub modelling the two Milvus behaviours that matter: a
   request above `enumerate/max-page` is refused, and a page comes back in no
   particular order. No live Milvus, no redefined vars."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-milvus.relocate.enumerate :as enumerate]
            [hive-milvus.store.entries :as entries]))

(defn- row-id [i]
  (format "20260820%06d-abcdef01" i))

(defn- matches?
  "Interprets exactly the filter shapes the enumerator emits: an optional
   `id >= \"lo\"` and an optional `id < \"hi\"`, compared as strings."
  [filter-expr ^String id]
  (let [lo (second (re-find #"id >= \"([^\"]*)\"" filter-expr))
        hi (second (re-find #"id < \"([^\"]*)\"" filter-expr))]
    (and (or (nil? lo) (not (neg? (compare id lo))))
         (or (nil? hi) (neg? (compare id hi))))))

(defn- stub-fetch
  "FETCH over `ids` (default: `n` timestamp ids). Refuses a page above the
   cap, as Milvus does, and shuffles every answer. Records each requested page
   limit in `calls`."
  ([n calls] (stub-fetch n calls (mapv row-id (range n))))
  ([_n calls ids]
   (let [rows (mapv (fn [id] {:id id}) ids)
         rng  (java.util.Random. 42)]
     (fn [filter-expr limit]
       (swap! calls conj limit)
       (when (> limit enumerate/max-page)
         (throw (ex-info "invalid max query result window, limit exceeds 16384"
                         {:limit limit})))
       (let [hits (java.util.ArrayList. ^java.util.Collection
                   (filterv #(matches? filter-expr (:id %)) rows))]
         (java.util.Collections/shuffle hits rng)
         (vec (take limit hits)))))))

(deftest limit-above-cap-returns-rows-not-an-empty-failure-test
  (let [calls   (atom [])
        outcome (entries/collection-outcome (stub-fetch 30000 calls)
                                            "memory_x" "id != \"\"" 20000)]
    (is (nil? (:error outcome)) "no failure recorded")
    (is (= 20000 (count (:rows outcome))) "exactly :limit rows")
    (is (apply distinct? (map :id (:rows outcome))) "no row served twice")
    (is (every? #(<= % enumerate/max-page) @calls)
        "no page ever asked Milvus for more than its cap")))

(deftest limit-above-row-count-returns-every-row-test
  (let [calls   (atom [])
        outcome (entries/collection-outcome (stub-fetch 30000 calls)
                                            "memory_x" "id != \"\"" 50000)]
    (is (nil? (:error outcome)))
    (is (= (set (map row-id (range 30000)))
           (set (map :id (:rows outcome)))))))

(deftest ids-that-do-not-start-with-a-digit-are-all-enumerated-test
  (testing "more non-digit ids than one page holds: chunk ids and uuids"
    (let [chunks  (map #(str "ingest-chunk-" (format "%08x" %) "-" (mod % 7)) (range 20000))
          uuids   (repeatedly 20000 #(str (java.util.UUID/randomUUID)))
          stamped (map row-id (range 5000))
          ids     (vec (concat chunks uuids stamped))
          calls   (atom [])
          outcome (entries/collection-outcome (stub-fetch 0 calls ids)
                                              "memory_x" "id != \"\"" 100000)]
      (is (nil? (:error outcome)) "no failure recorded")
      (is (= (set ids) (set (map :id (:rows outcome)))) "every id, none missing")
      (is (apply distinct? (map :id (:rows outcome))) "no row served twice")
      (is (every? #(<= % enumerate/max-page) @calls)
          "no page ever asked Milvus for more than its cap"))))

(deftest limit-within-cap-is-one-fetch-test
  (let [calls   (atom [])
        outcome (entries/collection-outcome (stub-fetch 30000 calls)
                                            "memory_x" "id != \"\"" 100)]
    (is (= 100 (count (:rows outcome))))
    (is (= [100] @calls))))

(deftest an-unreadable-collection-is-reported-not-silent-test
  (testing "a bucket still full at max depth raises and lands in :error"
    (let [dense (fn [_ limit] (vec (repeat limit {:id "20260820000000-x"})))
          outcome (entries/collection-outcome dense "memory_x" "id != \"\"" 20000)]
      (is (= [] (:rows outcome)))
      (is (string? (:error outcome)))
      (is (= [{:collection "memory_x" :message (:error outcome)}]
             (:failed (entries/fan-out [outcome])))))))
