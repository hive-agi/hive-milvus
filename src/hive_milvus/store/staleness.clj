(ns hive-milvus.store.staleness
  "Staleness protocol helpers for MilvusMemoryStore."
  (:require [hive-milvus.store.health :refer [resilient]]
            [hive-milvus.store.lookup :as lookup]
            [hive-milvus.store.query :as query]
            [hive-milvus.store.routing :as routing]
            [hive-milvus.store.schema :as schema]
            [milvus-clj.api :as milvus]))

(defn update-staleness!
  [config-atom id staleness-opts]
  (resilient config-atom
    (when-let [coll-name (lookup/find-entry-collection config-atom id)]
      (let [{:keys [beta source depth]} staleness-opts]
        (query/update-entry-fields! coll-name id
          (cond-> {}
            beta   (assoc :staleness-beta beta)
            source (assoc :staleness-source (name source))
            depth  (assoc :staleness-depth depth)))))))

(defn get-stale-entries
  [config-atom threshold opts]
  (resilient config-atom
    (let [{:keys [project-id type]} opts
          colls (if type
                  [(:collection-name (routing/coll-for-type type))]
                  (lookup/known-collections config-atom))
          filter-expr (schema/build-filter-expr {:project-id project-id :type type
                                                 :include-expired? true})
          fan-out (mapcat
                   (fn [coll-name]
                     (try
                       @(milvus/query-scalar coll-name
                          {:filter (or filter-expr "id != \"\"")
                           :output-fields schema/default-read-fields
                           :limit 10000})
                       (catch Exception _ [])))
                   colls)]
      (->> (mapv schema/record->entry fan-out)
           (filter #(> (schema/staleness-probability %) threshold))
           vec))))

(defn propagate-staleness!
  [config-atom source-id depth]
  (resilient config-atom
    (when-let [src-coll (lookup/find-entry-collection config-atom source-id)]
      (when-let [entry (query/get-entry-by-id src-coll source-id)]
        (let [kg-outgoing (:kg-outgoing entry)]
          (reduce (fn [cnt dep-id]
                    (if (and dep-id (seq dep-id))
                      (if-let [dep-coll (lookup/find-entry-collection config-atom dep-id)]
                        (if-let [dep (query/get-entry-by-id dep-coll dep-id)]
                          (do (query/update-entry-fields! dep-coll dep-id
                                {:staleness-beta (inc (or (:staleness-beta dep) 1))
                                 :staleness-source "transitive"
                                 :staleness-depth (inc depth)})
                              (inc cnt))
                          cnt)
                        cnt)
                      cnt))
                  0
                  kg-outgoing))))))
