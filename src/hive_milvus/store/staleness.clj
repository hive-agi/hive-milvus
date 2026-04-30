(ns hive-milvus.store.staleness
  "Staleness protocol helpers for MilvusMemoryStore."
  (:require [hive-milvus.store.health :refer [resilient]]
            [hive-milvus.store.query :as query]
            [hive-milvus.store.schema :as schema]
            [milvus-clj.api :as milvus]))

(defn- coll-name
  [config-atom]
  (:collection-name @config-atom "hive_mcp_memory"))

(defn update-staleness!
  [config-atom id staleness-opts]
  (resilient config-atom
    (let [coll-name (coll-name config-atom)
          {:keys [beta source depth]} staleness-opts]
      (query/update-entry-fields! coll-name id
        (cond-> {}
          beta   (assoc :staleness-beta beta)
          source (assoc :staleness-source (name source))
          depth  (assoc :staleness-depth depth))))))

(defn get-stale-entries
  [config-atom threshold opts]
  (resilient config-atom
    (let [coll-name (:collection-name @config-atom "hive_mcp_memory")
          {:keys [project-id type]} opts
          filter-expr (schema/build-filter-expr {:project-id project-id :type type
                                                 :include-expired? true})
          results @(milvus/query-scalar coll-name
                     {:filter (or filter-expr "id != \"\"")
                      :output-fields schema/default-read-fields
                      :limit 10000})]
      (->> (mapv schema/record->entry results)
           (filter #(> (schema/staleness-probability %) threshold))
           vec))))

(defn propagate-staleness!
  [config-atom source-id depth]
  (resilient config-atom
    (let [coll-name (coll-name config-atom)]
      (when-let [entry (query/get-entry-by-id coll-name source-id)]
        (let [kg-outgoing (:kg-outgoing entry)]
          (reduce (fn [cnt dep-id]
                    (if (and dep-id (seq dep-id))
                      (if-let [dep (query/get-entry-by-id coll-name dep-id)]
                        (do (query/update-entry-fields! coll-name dep-id
                              {:staleness-beta (inc (or (:staleness-beta dep) 1))
                               :staleness-source "transitive"
                               :staleness-depth (inc depth)})
                            (inc cnt))
                        cnt)
                      cnt))
                  0
                  kg-outgoing))))))
