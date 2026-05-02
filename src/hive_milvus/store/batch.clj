(ns hive-milvus.store.batch
  "Batch protocol helpers for MilvusMemoryStore."
  (:require [hive-milvus.store.health :refer [resilient]]
            [hive-milvus.store.lookup :as lookup]
            [hive-milvus.store.schema :as schema]
            [milvus-clj.api :as milvus]))

(defn get-entries
  [config-atom ids]
  (let [ids (vec (distinct (remove nil? ids)))]
    (when (seq ids)
      (resilient config-atom
        (let [colls (lookup/known-collections config-atom)
              fan-out (mapcat
                       (fn [coll-name]
                         (try
                           @(milvus/get coll-name ids
                              :consistency-level :bounded
                              :include schema/default-read-fields)
                           (catch Exception _ [])))
                       colls)
              ;; Dedupe by :id — first hit wins (default 768-d collection
              ;; comes first, so legacy entries take precedence over any
              ;; orphaned post-migration duplicates).
              by-id (reduce (fn [m row]
                              (if (contains? m (:id row)) m
                                  (assoc m (:id row) row)))
                            {}
                            fan-out)]
          (mapv schema/record->entry (vals by-id)))))))
