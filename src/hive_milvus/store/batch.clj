(ns hive-milvus.store.batch
  "Batch protocol helpers for MilvusMemoryStore."
  (:require [hive-milvus.store.health :refer [resilient]]
            [hive-milvus.store.schema :as schema]
            [milvus-clj.api :as milvus]))

(defn get-entries
  [config-atom ids]
  (let [ids (vec (distinct (remove nil? ids)))]
    (when (seq ids)
      (resilient config-atom
        (let [coll-name (:collection-name @config-atom "hive_mcp_memory")
              results   @(milvus/get coll-name ids
                           :consistency-level :bounded
                           :include schema/default-read-fields)]
          (mapv schema/record->entry results))))))
