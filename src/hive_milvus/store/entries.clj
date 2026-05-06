(ns hive-milvus.store.entries
  "Core entry CRUD, query, search, expiry, and status helpers."
  (:require [hive-mcp.embeddings.service :as embed-svc]
            [hive-milvus.store.health :refer [resilient]]
            [hive-milvus.store.index :as index]
            [hive-milvus.store.lookup :as lookup]
            [hive-milvus.store.query :as query]
            [hive-milvus.store.routing :as routing]
            [hive-milvus.store.schema :as schema]
            [milvus-clj.api :as milvus]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [hive-milvus.relocate.pipeline :as reloc-pipeline]
            [hive-dsl.result :as r]))

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
    (let [coll-name (routing/ensure-routed! (:type entry))
          record    (schema/entry->record entry coll-name)]
      @(milvus/add coll-name [record] :upsert? true)
      (:id record))))

(defn get-entry
  [config-atom id]
  (resilient config-atom
    (some (fn [coll]
            (try (query/get-entry-by-id coll id)
                 (catch Exception _ nil)))
          (lookup/known-collections config-atom))))

(defn update-fields-keep-embedding!
  "Update entry fields without re-embedding.

   Reads the existing record (including its :embedding vector via
   query-scalar), merges `updates`, and upserts in place via
   `entry->record-pure` with the retrieved vector. Suitable for
   metadata-only changes (e.g. :kg-incoming back-edge bookkeeping)
   where re-running the embedder on unchanged content is wasted work
   — and on 4096d Venice that waste blows past the 30 s memory-write
   timeout when an add fans out updates to multiple KG targets.

   Returns the merged entry on success, nil if id not found in any
   known collection."
  [config-atom id updates]
  (resilient config-atom
    (some
      (fn [coll]
        (try
          (let [rows @(milvus/query-scalar coll
                        {:filter            (str "id == \"" id "\"")
                         :limit             1
                         :output-fields     ["id" "embedding" "type" "tags" "content"
                                             "content_hash" "created" "updated"
                                             "duration" "expires"
                                             "access_count" "helpful_count" "unhelpful_count"
                                             "project_id"]
                         :consistency-level :strong})]
            (when-let [row (first rows)]
              (let [existing  (schema/record->entry row)
                    embedding (:embedding row)
                    merged    (-> existing
                                  (merge updates)
                                  (assoc :id id :updated (schema/now-iso)))
                    record    (schema/entry->record-pure merged coll embedding)]
                @(milvus/add coll [record] :upsert? true)
                merged)))
          (catch Exception _ nil)))
      (lookup/known-collections config-atom))))

(defn update-entry!
  "Update an entry's fields. Routing-aware via the CPPB-layered
   pipeline — when the merged entry's target collection differs from
   its current collection, the pipeline relocates it transparently.

   Delegates to `hive-milvus.relocate.pipeline/relocate-update`, which
   handles the COLLECT → PROMOTE → BOUNDARY flow with proper Result
   tracking. This wrapper unwraps the pipeline's r/ok / r/err back
   into the legacy raw-map shape callers expect: returns the merged
   entry on success, nil when `id` is unknown, or a raw err map for
   downstream errors.

   Migration path: callers that want railway-tracked errors should
   call `reloc-pipeline/relocate-update` directly instead of this
   facade."
  [config-atom id updates]
  (let [res (reloc-pipeline/relocate-update config-atom id updates)]
    (cond
      (r/ok? res)
      (:ok res)

      (= :collector/not-found (:error res))
      nil

      :else
      {:error (:error res) :id id :detail (dissoc res :error)})))

(defn delete-entry!
  [config-atom id]
  (resilient config-atom
    (doseq [coll (lookup/known-collections config-atom)]
      (try @(milvus/delete coll [id])
           (catch Exception _ nil)))
    true))

(defn target-collection-for
  "Resolve the canonical Milvus collection for `entry` per current
   routing config (per-type → per-dim). Returns the collection name
   string. Implements `IMemoryStoreWithRouting/target-collection-for`."
  [_config-atom entry]
  (-> entry routing/coll-for-entry :collection-name))

(defn relocate-entry!
  "Move entry `id` from its current collection to the canonical target.
   Implements `IMemoryStoreWithRouting/relocate-entry!`.

   Delegates to `hive-milvus.relocate.pipeline/relocate-one`, which is
   the CPPB-layered (Collect → Promote → Boundary) implementation.
   This wrapper unwraps the pipeline's r/ok / r/err result back into
   the legacy raw-map shape callers expect:

     {:moved? true  :from src :to target :id id}                on move
     {:moved? false :from src :to target :id id}                on no-op
     {:moved? false :from nil :to nil :id id :reason :not-found}
        when the id resolves to no collection
     {:moved? false :error <category> :id id :detail err-data}
        when the pipeline returns r/err for any other reason

   Migration path: callers that want railway-tracked errors should
   call `reloc-pipeline/relocate-one` directly instead of this
   facade — they get an r/ok / r/err with full error context."
  [config-atom id]
  (let [res (reloc-pipeline/relocate-one config-atom id)]
    (cond
      (r/ok? res)
      (:ok res)

      (= :collector/not-found (:error res))
      {:moved? false :from nil :to nil :id id :reason :not-found}

      :else
      {:moved? false :error (:error res) :id id
       :detail (dissoc res :error)})))

(defn query-entries
  "Fan out a scalar-filter query across every known collection.

   Per-collection failures (transient transport drops, missing index,
   schema drift on legacy collections) are isolated: the offending coll
   contributes [] and the others return their hits. The failure is
   logged at WARN so callers don't read a silent empty result as
   `:limit not respected` (the silent-swallow used to surface as the
   user-visible bug 20260503012357-7d008e50)."
  [config-atom opts]
  (resilient config-atom
    (let [colls (lookup/known-collections config-atom)
          {:keys [limit output-fields order-by]
           :or {limit 100}} opts
          fields (or output-fields schema/default-read-fields)
          filter-expr (schema/build-filter-expr opts)
          fan-out (mapcat
                   (fn [coll-name]
                     (try
                       (index/ensure-scalar-indexes! coll-name)
                       (if filter-expr
                         @(milvus/query-scalar coll-name
                            {:filter filter-expr :limit limit
                             :output-fields fields
                             :consistency-level :bounded})
                         @(milvus/query-scalar coll-name
                            {:filter "id != \"\"" :limit limit
                             :output-fields fields
                             :consistency-level :bounded}))
                       (catch Exception e
                         (log/warn "milvus query-entries: collection"
                                   coll-name "failed —" (.getMessage e)
                                   "(returning [] for this coll)")
                         [])))
                   colls)]
      (-> (mapv schema/record->entry fan-out)
          (apply-order-by order-by)
          (cond->> (> (count fan-out) limit) (take limit))
          vec))))

(defn search-similar
  [config-atom query-text opts]
  (resilient config-atom
    (let [{:keys [limit type project-ids exclude-tags]
           :or   {limit 10}} opts
          ;; Vector search must run per-collection — query embedding's
          ;; dimension must match the collection's schema. With no type
          ;; opt we fan out, embed once per dim, and concat results
          ;; ranked by Milvus's per-collection L2 score.
          colls       (if type
                        [(:collection-name (routing/coll-for-type type))]
                        (lookup/known-collections config-atom))
          filter-expr (schema/build-filter-expr
                        (cond-> {:include-expired? false}
                          type         (assoc :type type)
                          project-ids  (assoc :project-ids project-ids)
                          exclude-tags (assoc :exclude-tags exclude-tags)))
          fan-out     (mapcat
                       (fn [coll-name]
                         (try
                           (let [query-vec (embed-svc/embed-for-collection coll-name query-text)]
                             @(milvus/query coll-name
                                (cond-> {:vector query-vec :limit limit
                                         :output-fields schema/default-read-fields}
                                  filter-expr (assoc :filter filter-expr))))
                           (catch Exception _ [])))
                       colls)]
      (->> fan-out
           (mapv schema/record->entry)
           (sort-by :distance)
           (take limit)
           vec))))

(defn supports-semantic-search?
  [config-atom]
  ;; A provider for any known collection means semantic search is wired.
  (some embed-svc/provider-available-for? (lookup/known-collections config-atom)))

(defn cleanup-expired!
  [config-atom]
  (resilient config-atom
    (let [now (schema/now-iso)]
      (reduce
       (fn [total coll-name]
         (try
           (let [expired @(milvus/query-scalar coll-name
                            {:filter (str "expires != \"\" and expires < \"" now "\"")
                             :output-fields ["id"]
                             :limit 10000})]
             (when (seq expired)
               (let [ids (mapv :id expired)]
                 @(milvus/delete coll-name ids)
                 (log/info "Cleaned up" (count ids) "expired entries from Milvus collection" coll-name)))
             (+ total (count expired)))
           (catch Exception _ total)))
       0
       (lookup/known-collections config-atom)))))

(defn entries-expiring-soon
  [config-atom days opts]
  (resilient config-atom
    (let [now        (java.time.ZonedDateTime/now (java.time.ZoneId/systemDefault))
          horizon    (str (.plusDays now days))
          now-str    (str now)
          filter-cls (cond-> [(str "expires != \"\" and expires > \"" now-str
                               "\" and expires < \"" horizon "\"")]
                       (:project-id opts)
                       (conj (str "project_id == \"" (:project-id opts) "\"")))]
      (mapcat
       (fn [coll-name]
         (try
           (mapv schema/record->entry
                 @(milvus/query-scalar coll-name
                    {:filter (str/join " and " filter-cls)
                     :output-fields schema/default-read-fields
                     :limit 1000}))
           (catch Exception _ [])))
       (lookup/known-collections config-atom)))))

(defn find-duplicate
  [config-atom type content-hash opts]
  (resilient config-atom
    (let [colls       (if type
                        [(:collection-name (routing/coll-for-type type))]
                        (lookup/known-collections config-atom))
          filter-cls  (cond-> [(str "type == \"" (name type) "\"")
                               (str "content_hash == \"" content-hash "\"")]
                        (:project-id opts)
                        (conj (str "project_id == \"" (:project-id opts) "\"")))
          filter-expr (str/join " and " filter-cls)]
      (some (fn [coll-name]
              (try
                (when-let [hit (first @(milvus/query-scalar coll-name
                                         {:filter filter-expr
                                          :output-fields schema/default-read-fields
                                          :limit 1}))]
                  (schema/record->entry hit))
                (catch Exception _ nil)))
            colls))))

(defn store-status
  [config-atom]
  (resilient config-atom
    (let [coll-counts
          (reduce
           (fn [acc coll-name]
             (try
               (let [n (count @(milvus/query-scalar coll-name
                                 {:filter "id != \"\""
                                  :output-fields ["id"]
                                  :limit 100000}))]
                 (assoc acc coll-name n))
               (catch Exception _ acc)))
           {}
           (lookup/known-collections config-atom))]
      {:backend          "milvus"
       :configured?      (milvus/connected?)
       :entry-count      (try (reduce + 0 (vals coll-counts))
                              (catch Exception _ nil))
       :collections      coll-counts
       :supports-search? (milvus/connected?)})))

(defn reset-store!
  [config-atom]
  (resilient config-atom
    (doseq [coll-name (lookup/known-collections config-atom)]
      (try
        (when @(milvus/has-collection coll-name)
          @(milvus/drop-collection coll-name)
          (index/invalidate-loaded-collection! coll-name))
        (catch Exception _ nil)))
    true))