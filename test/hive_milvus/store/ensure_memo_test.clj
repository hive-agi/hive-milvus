(ns hive-milvus.store.ensure-memo-test
  "ensure-collection! consults its loaded memo before any RPC: N writes to
   one collection probe has-collection once."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-milvus.fake-milvus :as fake]
            [hive-milvus.recording-milvus :as rec]
            [hive-milvus.store.index :as index]
            [hive-milvus.store :as store]
            [hive-spi.memory.ports :as ports]))

(deftest memo-hit-skips-has-collection
  (let [{:keys [calls restore]} (rec/install!)]
    (try
      (dotimes [_ 10] (index/ensure-collection! fake/collection 768))
      (is (= 1 (rec/count-op calls :has-collection)))
      (testing "invalidation forces one fresh probe"
        (index/invalidate-loaded-collection! fake/collection)
        (index/ensure-collection! fake/collection 768)
        (is (= 2 (rec/count-op calls :has-collection))))
      (finally (restore)))))

(deftest n-writes-one-has-collection
  (let [{:keys [calls restore]} (rec/install!)
        s (store/create-store)]
    (try
      (dotimes [i 25]
        (is (string? (ports/add-entry! s {:type :note :content (str "c" i)}))))
      (is (= 1 (rec/count-op calls :has-collection fake/collection)))
      (is (= 25 (rec/count-op calls :upsert fake/collection)))
      (finally (restore)))))
