(ns hive-milvus.store.entries
  "Core entry CRUD, query, search, expiry, and status helpers."
  (:require [hive-mcp.embeddings.service :as embed-svc]
            [hive-milvus.store.health :refer [resilient]]
            [hive-milvus.store.index :as index]
            [hive-milvus.store.query :as query]
            [hive-milvus.store.schema :as schema]
            [milvus-clj.api :as milvus]
            [clojure.string :as str]
            [taoensso.timbre :as log]))

(defn- coll-name
  [config-atom]
  (:collection-name @config-atom "hive_mcp_memory"))

(defn- apply-order-by
  [entries order-by]
  (if-let [[field direction] order-by]
    (let [cmp (if (= direction :desc)
                #(compare %2 %1)
                compare)]
      (vec (sort-by field cmp entries)))
    entries))

(defn add-entry!
  [config-atom entry]
  (resilient config-atom
    (let [coll-name (coll-name config-atom)
          record    (schema/entry->record entry coll-name)]
      @(milvus/add coll-name [record] :upsert? true)
      (:id record))))

(defn get-entry
  [config-atom id]
  (resilient config-atom
    (query/get-entry-by-id (coll-name config-atom) id)))

(defn update-entry!
  [config-atom id updates]
  (resilient config-atom
    (query/update-entry-fields! (coll-name config-atom) id updates)))

(defn delete-entry!
  [config-atom id]
  (resilient config-atom
    @(milvus/delete (coll-name config-atom) [id])
    true))

(defn query-entries
  [config-atom opts]
  (resilient config-atom
    (let [coll-name (coll-name config-atom)
          _         (index/ensure-scalar-indexes! coll-name)
          {:keys [limit output-fields order-by]
           :or {limit 100}} opts
          fields (or output-fields schema/default-read-fields)
          filter-expr (schema/build-filter-expr opts)
          results (if filter-expr
                    @(milvus/query-scalar coll-name
                       {:filter filter-expr :limit limit
                        :output-fields fields
                        :consistency-level :bounded})
                    @(milvus/query-scalar coll-name
                       {:filter "id != \"\"" :limit limit
                        :output-fields fields
                        :consistency-level :bounded}))]
      (-> (mapv schema/record->entry results)
          (apply-order-by order-by)))))

(defn search-similar
  [config-atom query-text opts]
  (resilient config-atom
    (let [coll-name   (coll-name config-atom)
          {:keys [limit type project-ids exclude-tags]
           :or   {limit 10}} opts
          query-vec   (embed-svc/embed-for-collection coll-name query-text)
          filter-expr (schema/build-filter-expr
                        (cond-> {:include-expired? false}
                          type         (assoc :type type)
                          project-ids  (assoc :project-ids project-ids)
                          exclude-tags (assoc :exclude-tags exclude-tags)))
          results     @(milvus/query coll-name
                         (cond-> {:vector query-vec :limit limit
                                  :output-fields schema/default-read-fields}
                           filter-expr (assoc :filter filter-expr)))]
      (mapv schema/record->entry results))))

(defn supports-semantic-search?
  [config-atom]
  (embed-svc/provider-available-for? (coll-name config-atom)))

(defn cleanup-expired!
  [config-atom]
  (resilient config-atom
    (let [coll-name (coll-name config-atom)
          now       (schema/now-iso)
          expired   @(milvus/query-scalar coll-name
                       {:filter (str "expires != \"\" and expires < \"" now "\"")
                        :output-fields ["id"]
                        :limit 10000})]
      (when (seq expired)
        (let [ids (mapv :id expired)]
          @(milvus/delete coll-name ids)
          (log/info "Cleaned up" (count ids) "expired entries from Milvus")))
      (count expired))))

(defn entries-expiring-soon
  [config-atom days opts]
  (resilient config-atom
    (let [coll-name  (coll-name config-atom)
          now        (java.time.ZonedDateTime/now (java.time.ZoneId/systemDefault))
          horizon    (str (.plusDays now days))
          now-str    (str now)
          filter-cls (cond-> [(str "expires != \"\" and expires > \"" now-str
                               "\" and expires < \"" horizon "\"")]
                       (:project-id opts)
                       (conj (str "project_id == \"" (:project-id opts) "\"")))
          results    @(milvus/query-scalar coll-name
                        {:filter (str/join " and " filter-cls)
                         :output-fields schema/default-read-fields
                         :limit 1000})]
      (mapv schema/record->entry results))))

(defn find-duplicate
  [config-atom type content-hash opts]
  (resilient config-atom
    (let [coll-name  (coll-name config-atom)
          filter-cls (cond-> [(str "type == \"" (name type) "\"")
                              (str "content_hash == \"" content-hash "\"")]
                       (:project-id opts)
                       (conj (str "project_id == \"" (:project-id opts) "\"")))
          results    @(milvus/query-scalar coll-name
                        {:filter (str/join " and " filter-cls)
                         :output-fields schema/default-read-fields
                         :limit 1})]
      (when (seq results)
        (schema/record->entry (first results))))))

(defn store-status
  [config-atom]
  (resilient config-atom
    (let [coll-name (coll-name config-atom)]
      {:backend          "milvus"
       :configured?      (milvus/connected?)
       :entry-count      (try
                           (count @(milvus/query-scalar coll-name
                                     {:filter "id != \"\""
                                      :output-fields ["id"]
                                      :limit 100000}))
                           (catch Exception _ nil))
       :supports-search? (milvus/connected?)})))

(defn reset-store!
  [config-atom]
  (resilient config-atom
    (let [coll-name (coll-name config-atom)]
      (when @(milvus/has-collection coll-name)
        @(milvus/drop-collection coll-name))
      (index/invalidate-loaded-collection! coll-name)
      true)))
