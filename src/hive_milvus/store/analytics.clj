(ns hive-milvus.store.analytics
  "Analytics protocol helpers for MilvusMemoryStore."
  (:require [hive-milvus.store.health :refer [resilient]]
            [hive-milvus.store.query :as query]
            [hive-milvus.store.schema :as schema]))

(defn- coll-name
  [config-atom]
  (:collection-name @config-atom "hive_mcp_memory"))

(defn log-access!
  [config-atom id]
  (resilient config-atom
    (let [coll-name (coll-name config-atom)]
      (when-let [entry (query/get-entry-by-id coll-name id)]
        (query/update-entry-fields! coll-name id
          {:access-count (inc (or (:access-count entry) 0))})))))

(defn record-feedback!
  [config-atom id feedback]
  (resilient config-atom
    (let [coll-name (coll-name config-atom)]
      (when-let [entry (query/get-entry-by-id coll-name id)]
        (let [field     (schema/feedback->field feedback)
              new-count (inc (or (get entry field) 0))]
          (query/update-entry-fields! coll-name id {field new-count}))))))

(defn get-helpfulness-ratio
  [config-atom id]
  (resilient config-atom
    (let [coll-name (coll-name config-atom)]
      (when-let [entry (query/get-entry-by-id coll-name id)]
        (schema/helpfulness-map entry)))))
