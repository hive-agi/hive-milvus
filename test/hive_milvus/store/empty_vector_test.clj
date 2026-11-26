(ns hive-milvus.store.empty-vector-test
  "Friction 3b6e4010: an embedder that answers an empty or nil vector must
   never reach Milvus as a success. Measured through the embedding port (a
   fake IEmbedder installed with `port/set-embedder!`) over the recording
   in-memory milvus-clj client: no live Milvus, no redefined vars."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-dsl.result :as r]
            [hive-milvus.embed.port :as port]
            [hive-milvus.embedder :as embedder]
            [hive-milvus.recording-milvus :as rec]
            [hive-milvus.store :as store]
            [hive-milvus.store.entries :as entries]
            [hive-milvus.store.schema :as schema]
            [hive-milvus.test-isolation :as iso]
            [hive-spi.memory.contract :as contract]
            [hive-spi.memory.ports :as ports]))

(def ^:private c2560 "hive_mcp_memory_2560d")

(def ^:private routing
  {:collection-name c2560 :dimension 2560 :max-tokens 8192 :provider-key :fake})

(defn- answer
  "What the fake provider answers for CONTENT: \"EMPTY\" an empty vector,
   \"NIL\" a nil one, anything else a full 2560-wide vector."
  [content]
  (case content
    "EMPTY" (r/ok [])
    "NIL"   (r/ok nil)
    (r/ok (vec (repeat 2560 0.25)))))

(defrecord FakeEmbedder [calls]
  port/IEmbedder
  (-embed-entry [_ _entry coll content]
    (swap! calls conj [:single coll])
    (answer content))
  (-embed-text [_ _coll _text] (vec (repeat 2560 0.25)))
  (-routing-for-type [_ _t] routing)
  (-routing-for-type+size [_ _t _c] routing)
  (-no-embed-type? [_ _t] false)
  (-collection-names [_] [c2560])
  (-dimension-for-collection [_ _c] 2560)
  (-configured-collection-names [_] [c2560])
  (-provider-available-for? [_ _c] true)
  (-collection-backed? [_ _c] true))

(defrecord FakeBatchEmbedder [calls]
  port/IEmbedder
  (-embed-entry [_ _entry coll content]
    (swap! calls conj [:single coll])
    (answer content))
  (-embed-text [_ _coll _text] (vec (repeat 2560 0.25)))
  (-routing-for-type [_ _t] routing)
  (-routing-for-type+size [_ _t _c] routing)
  (-no-embed-type? [_ _t] false)
  (-collection-names [_] [c2560])
  (-dimension-for-collection [_ _c] 2560)
  (-configured-collection-names [_] [c2560])
  (-provider-available-for? [_ _c] true)
  (-collection-backed? [_ _c] true)
  port/IEmbedderBatch
  (-embed-entries [_ _entries coll contents]
    (swap! calls conj [:batch coll (count contents)])
    (mapv answer contents)))

(defn- with-embedder
  "Run (f calls) with a fake embedder installed through the port."
  [batch? f]
  (let [prev  (port/current)
        calls (atom [])]
    (port/set-embedder! (if batch? (->FakeBatchEmbedder calls) (->FakeEmbedder calls)))
    (try (f calls) (finally (port/set-embedder! prev)))))

(use-fixtures :each iso/isolate-resilience)

(deftest embed-for-entry-refuses-an-empty-or-nil-vector
  (with-embedder false
    (fn [_]
      (doseq [content ["EMPTY" "NIL"]]
        (let [res (embedder/embed-for-entry {:type :note :content content} c2560)]
          (is (r/err? res) content)
          (is (= :embedder/empty-vector (:error res)) content)
          (is (= c2560 (:collection res)))
          (is (= :note (:type res)))))
      (testing "blank content is no vector for a 2560-d collection either"
        (is (= :embedder/empty-vector
               (:error (embedder/embed-for-entry {:type :note :content "  "} c2560)))))
      (testing "a full vector still passes"
        (is (= 2560 (count (:ok (embedder/embed-for-entry {:type :note :content "x"} c2560)))))))))

(deftest embed-for-entries-refuses-each-empty-or-nil-vector-alone
  (doseq [batch? [true false]]
    (with-embedder batch?
      (fn [_]
        (let [res (embedder/embed-for-entries
                   [{:type :note :content "a"}
                    {:type :note :content "EMPTY"}
                    {:type :note :content "NIL"}
                    {:type :note :content "b"}]
                   c2560)]
          (is (= 4 (count res)))
          (is (r/ok? (res 0)) (str "batch? " batch?))
          (is (= :embedder/empty-vector (:error (res 1))) (str "batch? " batch?))
          (is (= :embedder/empty-vector (:error (res 2))) (str "batch? " batch?))
          (is (r/ok? (res 3)) (str "batch? " batch?)))))))

(deftest an-unknown-dimension-still-never-yields-a-usable-empty-vector
  (is (= :embedder/empty-vector
         (:error (embedder/usable-embedding (r/ok []) "custom" {:type :note}))))
  (is (= :embedder/empty-vector
         (:error (embedder/usable-embedding (r/ok nil) "custom" {:type :note}))))
  (is (= (r/ok [1.0]) (embedder/usable-embedding (r/ok [1.0]) "custom" {:type :note})))
  (let [err (r/err :embedder/embed-failed {:message "boom"})]
    (is (= err (embedder/usable-embedding err "custom" {:type :note})))))

(deftest the-noop-embedder-answers-no-provider
  (let [prev (port/current)]
    (port/reset-embedder!)
    (try
      (let [one  (port/embed-entry {:type :note} c2560 "text")
            many (port/embed-entries [{:type :note} {:type :note}] c2560 ["a" "b"])]
        (is (= :embedder/no-provider (:error one)))
        (is (string? (:hint one)))
        (is (= [:embedder/no-provider :embedder/no-provider] (mapv :error many)))
        (is (= :embedder/no-provider
               (:error (embedder/embed-for-entry {:type :note :content "text"} c2560)))))
      (finally (port/set-embedder! prev)))))

(deftest a-record-is-never-built-on-an-empty-embedding
  (doseq [emb [nil []]]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"empty embedding"
                          (schema/entry->record-pure {:id "e" :content "x"} c2560 emb))
        (pr-str emb)))
  (is (= [0.5] (:embedding (schema/entry->record-pure {:id "e" :content "x"} c2560 [0.5]))))
  (testing "the scalar fields the size guard reads carry no vector"
    (is (not (contains? (schema/entry->scalars {:id "e" :content "x"}) :embedding)))))

(defn- stored-records
  "Every record the in-memory client holds, by collection."
  []
  (let [slot @#'milvus-clj.api/default-client]
    (:db (:inner @slot))))

(deftest add-entries-fails-only-the-empty-vector-entries
  (doseq [batch? [true false]]
    (let [{:keys [calls restore]} (rec/install!)]
      (try
        (with-embedder batch?
          (fn [ecalls]
            (let [s   (store/create-store {})
                  es  [{:id "a" :type :note :content "one"}
                       {:id "b" :type :note :content "EMPTY"}
                       {:id "c" :type :note :content "two"}
                       {:id "d" :type :note :content "NIL"}
                       {:id "e" :type :note :content "three"}]
                  res (ports/add-entries! s es)
                  db  @(stored-records)]
              (is (contract/add-entries-result? 5 res))
              (is (= ["a" "c" "e"] [(res 0) (res 2) (res 4)]) (str "batch? " batch?))
              (doseq [i [1 3]]
                (is (contract/failure? (res i)) (str "batch? " batch? " entry " i))
                (is (= :embedder/empty-vector (:error (res i))))
                (is (= (:id (es i)) (:id (res i)))))
              (testing "one upsert of the usable entries; no fallback to single writes"
                (is (= [[:upsert c2560 3]] (filterv #(= :upsert (first %)) @calls)))
                (is (zero? (rec/count-op calls :insert))))
              (when batch?
                (is (= [[:batch c2560 5]] @ecalls) "one batch embed, no per-entry retries"))
              (is (= #{"a" "c" "e"} (set (keys (get db c2560)))))
              (is (every? #(= 2560 (count (:embedding %))) (vals (get db c2560)))
                  "no empty vector was ever upserted"))))
        (finally (restore))))))

(deftest add-entries-with-every-vector-empty-sends-nothing
  (let [{:keys [calls restore]} (rec/install!)]
    (try
      (with-embedder true
        (fn [_]
          (let [res (entries/add-entries! (:config-atom (store/create-store {}))
                                          [{:id "x" :type :note :content "EMPTY"}
                                           {:id "y" :type :note :content "NIL"}])]
            (is (every? contract/failure? res))
            (is (zero? (rec/count-op calls :upsert))))))
      (finally (restore)))))

(deftest add-entry-of-an-empty-vector-is-never-sent
  (let [{:keys [calls restore]} (rec/install!)]
    (try
      (with-embedder false
        (fn [_]
          (let [s (store/create-store {})]
            (is (thrown? clojure.lang.ExceptionInfo
                         (ports/add-entry! s {:id "z" :type :note :content "EMPTY"})))
            (is (zero? (rec/count-op calls :upsert)))
            (is (= "w" (ports/add-entry! s {:id "w" :type :note :content "fine"}))))))
      (finally (restore)))))
