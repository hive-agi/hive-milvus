(ns hive-milvus.store.analytics
  "Analytics protocol helpers for MilvusMemoryStore."
  (:require [hive-milvus.store.health :refer [resilient]]
            [hive-milvus.store.lookup :as lookup]
            [hive-milvus.store.query :as query]
            [hive-milvus.store.schema :as schema]))

(defn log-access!
  [config-atom id]
  (resilient config-atom
    (when-let [coll-name (lookup/find-entry-collection config-atom id)]
      (when-let [entry (query/get-entry-by-id coll-name id)]
        (query/update-entry-fields! coll-name id
          {:access-count (inc (or (:access-count entry) 0))})))))

(defn record-feedback!
  [config-atom id feedback]
  (resilient config-atom
    (when-let [coll-name (lookup/find-entry-collection config-atom id)]
      (when-let [entry (query/get-entry-by-id coll-name id)]
        (let [field     (schema/feedback->field feedback)
              new-count (inc (or (get entry field) 0))]
          (query/update-entry-fields! coll-name id {field new-count}))))))

(defn get-helpfulness-ratio
  [config-atom id]
  (resilient config-atom
    (when-let [coll-name (lookup/find-entry-collection config-atom id)]
      (when-let [entry (query/get-entry-by-id coll-name id)]
        (schema/helpfulness-map entry)))))
