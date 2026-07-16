(ns hive-milvus.store.entries
  "Core entry CRUD, query, search, expiry, and status helpers."
  (:require [hive-milvus.embed.port :as port]
            [hive-milvus.resilience.retry :refer [resilient]]
            [hive-milvus.store.index :as index]
            [hive-milvus.store.lookup :as lookup]
            [hive-milvus.store.query :as query]
            [hive-milvus.store.routing :as routing]
            [hive-milvus.store.schema :as schema]
            [milvus-clj.api :as milvus]
            [hive-spi.memory.ports :as ports]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [hive-milvus.relocate.pipeline :as reloc-pipeline]
            [hive-dsl.result :as r]
            [hive-milvus.store.search.pipeline :as search-pipeline]
            [hive-milvus.store.search.target :as search-target]
            [hive-milvus.store.search.boundary :as search-boundary]
            [malli.core :as m]))

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

(defn search-context
  "The live collaborators a semantic search runs against."
  [config-atom]
  (search-pipeline/context
    {:resolver (search-target/default-resolver config-atom)
     :embedder (search-boundary/collection-embedder)
     :searcher (search-boundary/milvus-vector-search)}))

(defn search-similar
  "Semantic search. Returns entries, best first.

   A target that fails is LOGGED, not silently dropped — a search that returns
   fewer hits because a collection errored used to report success."
  [config-atom query-text opts]
  (resilient config-atom
    (let [{:keys [results failed searched]}
          (search-pipeline/search (search-context config-atom)
                                  (assoc opts :text query-text))]
      (when (seq failed)
        (log/warn "search: target(s) failed" {:failed failed :searched searched}))
      results)))

(defn supports-semantic-search? [config-atom] (boolean (some port/provider-available-for? (lookup/known-collections config-atom))))

(defn cleanup-expired!
  [config-atom]
  (resilient config-atom
    (let [now       (schema/now-iso)
          protected (ports/protected-ids)]
      (reduce
       (fn [total coll-name]
         (try
           (let [expired @(milvus/query-scalar coll-name
                            {:filter (str "expires != \"\" and expires < \"" now "\"")
                             :output-fields ["id"]
                             :limit 10000})
                 ids     (into [] (comp (map :id) (remove protected)) expired)
                 spared  (- (count expired) (count ids))]
             (when (seq ids)
               @(milvus/delete coll-name ids)
               (log/info "Cleaned up" (count ids) "expired entries from Milvus collection" coll-name
                         (when (pos? spared)
                           (str "(" spared " spared by synthesis afterlife)"))))
             (+ total (count ids)))
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

(def CollectionCount
  [:map
   [:collection :string]
   [:count [:int {:min 0}]]])

(def CollectionCountFailure
  [:map
   [:collection :string]
   [:error :string]])

(def StoreStatus
  [:map
   [:backend [:= "milvus"]]
   [:configured? :boolean]
   [:entry-count [:maybe [:int {:min 0}]]]
   [:collections [:map-of :string [:int {:min 0}]]]
   [:errors [:vector CollectionCountFailure]]
   [:supports-search? :boolean]])

(defn- collection-count
  [collection-name]
  (let [row (first @(milvus/query-scalar
                     collection-name
                     {:filter "id != \"\""
                      :output-fields ["count(*)"]
                      :limit 1}))
        n (get row (keyword "count(*)"))]
    (if (nat-int? n)
      n
      (throw (ex-info "Milvus count aggregation returned no count"
                      {:type :milvus/count-unavailable
                       :collection collection-name
                       :row row})))))

(m/=> collection-count [:=> [:cat :string] [:int {:min 0}]])

(defn- collect-count
  [collection-name]
  (try
    {:collection collection-name
     :count (collection-count collection-name)}
    (catch Exception e
      {:collection collection-name
       :error (or (ex-message e) (str (class e)))})))

(defn store-status
  [config-atom]
  (resilient config-atom
    (let [outcomes (mapv collect-count (lookup/known-collections config-atom))
          errors (into [] (keep #(when (:error %) (select-keys % [:collection :error]))) outcomes)
          coll-counts (into {} (keep #(when-let [n (:count %)] [(:collection %) n])) outcomes)]
      {:backend "milvus"
       :configured? (boolean (milvus/connected?))
       :entry-count (when (empty? errors) (reduce + 0 (vals coll-counts)))
       :collections coll-counts
       :errors errors
       :supports-search? (and (boolean (milvus/connected?))
                              (supports-semantic-search? config-atom))})))

(m/=> store-status [:=> [:cat :any] StoreStatus])

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