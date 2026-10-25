(ns hive-milvus.store.write-guard-test
  "The write path's schema-size guard and time budget, exercised through the
   store port (`ports/add-entry!`, `add-entries!`) over the in-memory fake
   milvus-clj client: no live Milvus, no redefined vars."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-milvus.fake-milvus :as fake]
            [hive-milvus.resilience.budget :as budget]
            [hive-milvus.store :as store]
            [hive-milvus.store.entries :as entries]
            [hive-milvus.store.guard :as guard]
            [hive-milvus.test-isolation :as iso]
            [hive-spi.memory.ports :as ports]))

(def ^:private restore (atom nil))

(use-fixtures :once
  (fn [f]
    (reset! restore (fake/install!))
    (try (f) (finally (@restore)))))

(use-fixtures :each iso/isolate-resilience)

(defn- fake-store
  "A MilvusMemoryStore over a fresh in-memory client."
  []
  (@restore)
  (reset! restore (fake/install!))
  (store/create-store {:collection-name fake/collection}))

(def ^:private too-long
  "One byte over the :content VarChar limit."
  (apply str (repeat (inc (:content guard/varchar-limits)) "a")))

(def ^:private multibyte-over
  "Under the limit in chars, over it in UTF-8 bytes (3 bytes per char)."
  (apply str (repeat (inc (quot (:content guard/varchar-limits) 3)) "\u20ac")))

(deftest oversize-add-is-refused-not-sent
  (let [s   (fake-store)
        res (ports/add-entry! s {:id "big" :type :note :content too-long})]
    (is (false? (:success? res)))
    (is (= :milvus/payload-too-large (:error res)))
    (is (false? (:reconnecting? res)) "permanent: a queue drain drops it")
    (is (= [{:field :content :bytes (count too-long)
             :limit (:content guard/varchar-limits)}]
           (:oversize res)))
    (is (nil? (ports/get-entry s "big")) "nothing reached the collection")))

(deftest utf8-bytes-not-chars-decide
  (is (< (count multibyte-over) (:content guard/varchar-limits)))
  (let [res (ports/add-entry! (fake-store)
                              {:id "euro" :type :note :content multibyte-over})]
    (is (= :milvus/payload-too-large (:error res)))))

(deftest fitting-add-still-lands
  (let [s  (fake-store)
        id (ports/add-entry! s {:id "small" :type :note :content "fits"})]
    (is (= "small" id))
    (is (= "fits" (:content (ports/get-entry s "small"))))))

(deftest batch-refuses-only-the-oversize-entry
  (let [s   (fake-store)
        res (entries/add-entries! (:config-atom s)
                                  [{:id "a" :type :note :content "one"}
                                   {:id "b" :type :note :content too-long}
                                   {:id "c" :type :note :content "three"}])]
    (testing "results stay in input order, the refusal answers only for itself"
      (is (= "a" (res 0)))
      (is (= :milvus/payload-too-large (:error (res 1))))
      (is (= "b" (:id (res 1))))
      (is (= "c" (res 2))))
    (is (some? (ports/get-entry s "a")))
    (is (nil? (ports/get-entry s "b")))
    (is (some? (ports/get-entry s "c")))))

(deftest worst-case-write-fits-the-index-stage
  (is (< (budget/worst-case-write-ms) budget/stage-budget-ms)))
