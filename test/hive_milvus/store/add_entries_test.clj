(ns hive-milvus.store.add-entries-test
  "IMemoryStoreBatchWrite on MilvusMemoryStore, measured on a recording
   transport and a recording batch embedder."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.result :as r]
            [hive-milvus.embed.port :as port]
            [hive-milvus.recording-milvus :as rec]
            [hive-milvus.store :as store]
            [hive-spi.memory.contract :as contract]
            [hive-spi.memory.ports :as ports]))

(def ^:private c768 "hive_mcp_memory")
(def ^:private c1024 "hive_mcp_memory_1024d")

(defn- routing [t]
  (if (= "plan" (name (or t "note")))
    {:collection-name c1024 :dimension 1024 :max-tokens 8192 :provider-key :big}
    {:collection-name c768 :dimension 768 :max-tokens 2048 :provider-key :small}))

(defn- dim-of [coll] (if (= coll c1024) 1024 768))

(defn- recording-embedder
  "IEmbedder + IEmbedderBatch. `calls` collects [:batch coll n] and
   [:single coll]. Content \"BAD\" answers an embed error for that entry."
  [calls batch?]
  (let [one (fn [coll ct]
              (if (= "BAD" ct)
                (r/err :embedder/embed-failed {:content ct})
                (r/ok (vec (repeat (dim-of coll) 0.1)))))
        base {:single (fn [_ coll ct] (swap! calls conj [:single coll]) (one coll ct))}]
    (if batch?
      (reify
        port/IEmbedder
        (-embed-entry [_ e c ct] ((:single base) e c ct))
        (-embed-text [_ c _] (vec (repeat (dim-of c) 0.1)))
        (-routing-for-type [_ t] (routing t))
        (-routing-for-type+size [_ t _] (routing t))
        (-no-embed-type? [_ _] false)
        (-collection-names [_] [c768 c1024])
        (-dimension-for-collection [_ c] (dim-of c))
        (-configured-collection-names [_] [c768 c1024])
        (-provider-available-for? [_ _] true)
        (-collection-backed? [_ _] true)
        port/IEmbedderBatch
        (-embed-entries [_ _es coll contents]
          (swap! calls conj [:batch coll (count contents)])
          (mapv #(one coll %) contents)))
      (reify
        port/IEmbedder
        (-embed-entry [_ e c ct] ((:single base) e c ct))
        (-embed-text [_ c _] (vec (repeat (dim-of c) 0.1)))
        (-routing-for-type [_ t] (routing t))
        (-routing-for-type+size [_ t _] (routing t))
        (-no-embed-type? [_ _] false)
        (-collection-names [_] [c768 c1024])
        (-dimension-for-collection [_ c] (dim-of c))
        (-configured-collection-names [_] [c768 c1024])
        (-provider-available-for? [_ _] true)
        (-collection-backed? [_ _] true)))))

(defn- entries [n]
  (mapv (fn [i] {:id (str "e" i)
                 :type (if (even? i) :note :plan)
                 :content (str "content " i)})
        (range n)))

(defn- with-rig
  "Run (f calls ecalls store) on a fresh recording rig."
  [{:keys [batch? fail? cap] :or {batch? true}} f]
  (let [{:keys [calls restore]} (rec/install! {:fail? fail?})
        ecalls (atom [])
        prev   (port/current)]
    (port/set-embedder! (recording-embedder ecalls batch?))
    (try
      (f calls ecalls (store/create-store (cond-> {} cap (assoc :batch-records cap))))
      (finally (port/set-embedder! prev) (restore)))))

(deftest hundred-entries-two-collections
  (with-rig {}
    (fn [calls ecalls s]
      (let [es  (entries 100)
            res (ports/add-entries! s es)]
        (is (satisfies? ports/IMemoryStoreBatchWrite s))
        (is (contract/add-entries-result? 100 res))
        (is (= (mapv :id es) res) "ids in input order")
        (is (= 2 (rec/count-op calls :upsert)))
        (is (= 1 (rec/count-op calls :upsert c768)))
        (is (= 1 (rec/count-op calls :upsert c1024)))
        (is (= 1 (rec/count-op calls :has-collection c768)))
        (is (= 2 (count (filter #(= :batch (first %)) @ecalls))))
        (is (zero? (count (filter #(= :single (first %)) @ecalls))))
        (testing "RPCs per entry"
          (is (<= (/ (count @calls) 100.0) 0.1) (str "rpc calls: " @calls)))))))

(deftest chunked-by-batch-records
  (with-rig {:cap 16}
    (fn [calls _ s]
      (is (= 100 (count (ports/add-entries! s (entries 100)))))
      ;; 50 per collection, 16 per upsert => 4 chunks each
      (is (= 8 (rec/count-op calls :upsert))))))

(deftest falls-back-to-per-entry-embed
  (with-rig {:batch? false}
    (fn [calls ecalls s]
      (is (= (mapv :id (entries 10)) (ports/add-entries! s (entries 10))))
      (is (= 10 (count (filter #(= :single (first %)) @ecalls))))
      (is (= 2 (rec/count-op calls :upsert))))))

(deftest failing-record-keeps-the-others
  (testing "an embed failure answers only its own entry"
    (with-rig {}
      (fn [calls _ s]
        (let [es  (assoc-in (entries 10) [3 :content] "BAD")
              res (ports/add-entries! s es)]
          (is (contract/add-entries-result? 10 res))
          (is (contract/failure? (nth res 3)))
          (is (= (mapv :id (concat (take 3 es) (drop 4 es)))
                 (vec (concat (take 3 res) (drop 4 res)))))
          (is (= 2 (rec/count-op calls :upsert)))))))
  (testing "a refused upsert fails only its collection's entries"
    (with-rig {:fail? (fn [coll _] (= coll c1024))}
      (fn [_ _ s]
        (let [es  (entries 10)
              res (ports/add-entries! s es)]
          (is (contract/add-entries-result? 10 res))
          (doseq [[e r] (map vector es res)]
            (if (= :plan (:type e))
              (is (contract/failure? r) (str "plan entry " (:id e)))
              (is (= (:id e) r)))))))))

(deftest rpcs-per-entry-before-and-after
  (with-rig {}
    (fn [calls _ s]
      (doseq [e (entries 100)] (ports/add-entry! s e))
      (let [single (count @calls)]
        (reset! calls [])
        (ports/add-entries! s (map #(update % :id str "-b") (entries 100)))
        (let [batch (count @calls)]
          (println "RPCs/100 entries: add-entry! x100 =" single ", add-entries! =" batch)
          (is (< batch single)))))))
