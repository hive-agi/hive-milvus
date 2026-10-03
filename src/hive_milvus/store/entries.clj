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
            [hive-milvus.relocate.enumerate :as enumerate]
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

(defn scan-ids
  "Every entry id across the collections this store reads, each once; expired
   ones only with :include-expired?. Raises rather than truncating."
  [config-atom {:keys [include-expired?]}]
  (let [base (schema/build-filter-expr {:include-expired? include-expired?})]
    (->> (lookup/known-collections config-atom)
         (mapcat #(enumerate/all-ids % base))
         distinct
         vec)))

(defn add-entry!
  [config-atom entry]
  (resilient config-atom
    (let [coll-name (routing/ensure-routed! (:type entry))
          record    (schema/entry->record entry coll-name)]
      @(milvus/add coll-name [record] :upsert? true)
      (:id record))))

(def ReadFailure
  "One collection that could not answer a read."
  [:map {:closed true}
   [:collection :string]
   [:message [:maybe :string]]])

(defn- read-failure
  [coll ^Throwable e]
  {:collection coll :message (or (ex-message e) (str (class e)))})

(defn locate-entry
  "Ask each of `colls`, in order, for `id` through FETCH
   (`(fn [coll id] value-or-nil)`), stopping at the first value.

     r/ok value   some collection answered with it
     r/ok nil     EVERY collection answered and none holds `id` - absence
     r/err :milvus/read-incomplete {:id :failed [ReadFailure ...] :cause e}
                  no value was found AND at least one collection failed, so
                  absence cannot be told from an outage

   A hit wins over a failing neighbour. Never throws: the decision is data,
   and `located-value` is the boundary that raises it."
  [fetch colls id]
  (let [{:keys [found failed cause]}
        (reduce (fn [acc coll]
                  (try
                    (if-some [v (fetch coll id)]
                      (reduced (assoc acc :found v))
                      acc)
                    (catch Exception e
                      (-> acc
                          (update :failed conj (read-failure coll e))
                          (update :cause #(or % e))))))
                {:failed []}
                colls)]
    (cond
      (some? found) (r/ok found)
      (seq failed)  (r/err :milvus/read-incomplete
                           {:id id :failed failed :cause cause})
      :else         (r/ok nil))))

(m/=> locate-entry [:=> [:cat fn? [:sequential :string] :any] :map])

(defn located-value
  "The value a `locate-entry` result answers to a caller: the entry, nil for
   a true absence, or a raised ex-info for an incomplete read. The ex-info
   wraps the first collection's exception as its cause, so `resilient`
   classifies it (a transient cause retries, then answers the legacy
   `{:success? false ...}` map) and the failure is never reported as nil.
   ex-data carries `:hive-milvus/failed-collections`, the key `query-entries`
   uses."
  [res]
  (if (r/ok? res)
    (:ok res)
    (throw (ex-info (str "milvus read of " (pr-str (:id res)) " incomplete: "
                         (count (:failed res)) " collection(s) unreachable,"
                         " absence cannot be told from an outage")
                    {:hive-milvus/failed-collections (:failed res)
                     :id (:id res)}
                    (:cause res)))))

(defn get-entry
  "The entry `id`, or nil when every known collection answered and none
   holds it.

   A collection that cannot be read is NOT absence: when no collection
   holds `id` and one failed, the read raises inside `resilient`, which
   retries a transient failure once and then answers
   `{:success? false :errors [..] :reconnecting? true}` - never nil."
  [config-atom id]
  (resilient config-atom
    (located-value
     (locate-entry query/get-entry-by-id
                   (lookup/known-collections config-atom)
                   id))))

(defn- merge-keep-embedding!
  "FETCH port for `locate-entry`: read `id` from `coll` with its vector, merge
   `updates`, upsert in place. The merged entry, or nil when `coll` lacks `id`."
  [updates coll id]
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
        merged))))

(defn update-fields-keep-embedding!
  "Update entry fields without re-embedding.

   Reads the existing record (including its :embedding vector via
   query-scalar), merges `updates`, and upserts in place via
   `entry->record-pure` with the retrieved vector. Suitable for
   metadata-only changes (e.g. :kg-incoming back-edge bookkeeping)
   where re-running the embedder on unchanged content is wasted work
   — and on 4096d Venice that waste blows past the 30 s memory-write
   timeout when an add fans out updates to multiple KG targets.

   Returns the merged entry on success, nil when every known collection
   answered and none holds `id`. A collection that failed is not absence:
   see `locate-entry` / `located-value`, so the caller gets the legacy
   failure map from `resilient`, never a nil that reads as 'unknown id'."
  [config-atom id updates]
  (resilient config-atom
    (located-value
     (locate-entry (partial merge-keep-embedding! updates)
                   (lookup/known-collections config-atom)
                   id))))

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

(defn delete-everywhere
  "Delete `id` from every one of `colls` through DELETE (`(fn [coll id])`).

     r/ok true    every collection accepted the delete (an unknown id included)
     r/err :milvus/write-incomplete {:id :failed [ReadFailure ...] :cause e}
                  some collection did not, so the entry may survive there

   Every collection is attempted even after a failure. Never throws."
  [delete colls id]
  (let [{:keys [failed cause]}
        (reduce (fn [acc coll]
                  (try (delete coll id) acc
                       (catch Exception e
                         (-> acc
                             (update :failed conj (read-failure coll e))
                             (update :cause #(or % e))))))
                {:failed []}
                colls)]
    (if (seq failed)
      (r/err :milvus/write-incomplete {:id id :failed failed :cause cause})
      (r/ok true))))

(m/=> delete-everywhere [:=> [:cat fn? [:sequential :string] :any] :map])

(defn delete-entry!
  "Delete `id` from every known collection. true when every collection took
   the delete. A collection that failed means the delete did not land: it
   raises inside `resilient`, so the caller gets the hive-spi Failure map
   (`{:success? false ...}`) - never a `true` for a lost delete."
  [config-atom id]
  (resilient config-atom
    (located-value
     (delete-everywhere (fn [coll id] @(milvus/delete coll [id]))
                        (lookup/known-collections config-atom)
                        id))))

(defn target-collection-for
  "Resolve the canonical Milvus collection for `entry` per current
   routing config (per-type → per-dim). Returns the collection name
   string. Implements `IMemoryStoreWithRouting/target-collection-for`."
  [_config-atom entry]
  (-> entry routing/coll-for-entry :collection-name))

(defn relocate-entry!
  "Move entry `id` from its current collection to the canonical target.
   Implements `IMemoryStoreWithRouting/relocate-entry!`, and with `opts`
   `IMemoryStoreRoutingEmbedText/relocate-entry-with!`: `(:embed-text opts)`
   is embedded in place of the stored content, and never stored.

   Delegates to `hive-milvus.relocate.pipeline/relocate-one`, which is
   the CPPB-layered (Collect -> Promote -> Boundary) implementation.
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
   facade; they get an r/ok / r/err with full error context."
  ([config-atom id] (relocate-entry! config-atom id nil))
  ([config-atom id opts]
   (let [res (reloc-pipeline/relocate-one config-atom id (select-keys opts [:embed-text]))]
     (cond
       (r/ok? res)
       (:ok res)

       (= :collector/not-found (:error res))
       {:moved? false :from nil :to nil :id id :reason :not-found}

       :else
       {:moved? false :error (:error res) :id id
        :detail (dissoc res :error)}))))

(def FanOutOutcome
  "What one collection contributed to a fan-out: its rows, and — when it could
   not be reached — the message saying so. Rows and an error are not exclusive;
   a partial read may carry both."
  [:map {:closed true}
   [:collection :string]
   [:rows [:sequential :any]]
   [:error {:optional true} [:maybe :string]]])

(def FanOut
  "The rows a fan-out gathered, and the collections whose silence is unexplained
   by the data. `:failed` empty means every collection answered."
  [:map {:closed true}
   [:rows [:sequential :any]]
   [:failed [:sequential [:map {:closed true}
                          [:collection :string]
                          [:message [:maybe :string]]]]]])

(defn fan-out
  "Fold per-collection OUTCOMES into gathered rows and the failures that make an
   empty result unreliable.

   Pure, and the whole of the isolation policy: a failing collection contributes
   whatever rows it managed and is recorded in `:failed`, rather than sinking
   the query or vanishing."
  [outcomes]
  {:rows   (into [] (mapcat :rows) outcomes)
   :failed (into [] (keep (fn [{:keys [collection error]}]
                            (when error {:collection collection :message error})))
                 outcomes)})

(m/=> fan-out [:=> [:cat [:sequential FanOutOutcome]] FanOut])

(defn collection-outcome
  "One collection's FanOutOutcome for a scalar query: up to `limit` rows matching
   `filter-expr`, read through FETCH (`(fn [filter-expr page-limit] rows)`).

   A `limit` above Milvus's per-query cap is served in pages that each stay
   within it (`enumerate/rows-matching`), so a large limit returns rows, never
   a failure dressed as an empty result. A collection that cannot be read
   yields `:rows []` with its `:error`."
  [fetch coll-name filter-expr limit]
  (try
    {:collection coll-name
     :rows (enumerate/rows-matching fetch filter-expr limit)}
    (catch Exception e
      (log/warn "milvus query-entries: collection"
                coll-name "failed:" (ex-message e)
                "(returning [] for this coll)")
      {:collection coll-name :rows [] :error (or (ex-message e) (str (class e)))})))

(defn query-entries
  "Fan out a scalar-filter query across every known collection.

   Per-collection failures (transient transport drops, missing index,
   schema drift on legacy collections) are isolated: the offending coll
   contributes [] and the others return their hits. The failure is
   logged at WARN so callers don't read a silent empty result as
   `:limit not respected` (the silent-swallow used to surface as the
   user-visible bug 20260503012357-7d008e50).

   A log line is not something a caller can branch on, so when any coll
   failed the returned vector also carries

     ^{:hive-milvus/failed-collections [{:collection name :message str} ...]}

   Absent metadata means every collection answered, so an empty result is a
   fact about the DATA. Present metadata means the emptiness is partly an
   artifact of the failure, and a caller must not report it as 'nothing
   stored'.

   Effectful boundary only — the isolation policy itself is `fan-out`."
  [config-atom opts]
  (resilient config-atom
    (let [colls (lookup/known-collections config-atom)
          {:keys [limit output-fields order-by]
           :or {limit 100}} opts
          fields (or output-fields schema/default-read-fields)
          filter-expr (or (schema/build-filter-expr opts) "id != \"\"")
          outcomes (mapv
                    (fn [coll-name]
                      (let [indexed (delay (index/ensure-scalar-indexes! coll-name))]
                        (collection-outcome
                         (fn [filt page-limit]
                           @indexed
                           @(milvus/query-scalar coll-name
                              {:filter filt :limit page-limit
                               :output-fields fields
                               :consistency-level :bounded}))
                         coll-name filter-expr limit)))
                    colls)
          {:keys [rows failed]} (fan-out outcomes)]
      (cond-> (-> (mapv schema/record->entry rows)
                  (apply-order-by order-by)
                  (cond->> (> (count rows) limit) (take limit))
                  vec)
        (seq failed) (with-meta {:hive-milvus/failed-collections failed})))))

(defn search-context
  "The live collaborators a semantic search runs against."
  [config-atom]
  (search-pipeline/context
    {:resolver (search-target/default-resolver config-atom)
     :embedder (search-boundary/collection-embedder)
     :searcher (search-boundary/milvus-vector-search)}))

(defn search-with
  "Run a semantic search for `query-text` against the collaborators in `ctx`
   (see `search-pipeline/context`). Returns the entries, best first.

   A target that fails is logged AND carried on the returned vector as

     ^{:hive-milvus/failed-collections [{:collection name :message str} ...]}

   - the key and shape `query-entries` uses. Absent metadata means every
   target answered, so an empty result is a fact about the data; present
   metadata means the result is partial and must not be reported as
   'nothing matches'."
  [ctx query-text opts]
  (let [{:keys [results failed searched]}
        (search-pipeline/search ctx (assoc opts :text query-text))]
    (if (seq failed)
      (do (log/warn "search: target(s) failed" {:failed failed :searched searched})
          (with-meta (vec results)
            {:hive-milvus/failed-collections
             (mapv (fn [{:keys [collection error] :as f}]
                     {:collection collection
                      :message    (str (or (:message f) error))})
                   failed)}))
      (vec results))))

(defn search-similar
  "Semantic search. Returns entries, best first; a failed target is carried
   as `:hive-milvus/failed-collections` metadata (see `search-with`)."
  [config-atom query-text opts]
  (resilient config-atom
    (search-with (search-context config-atom) query-text opts)))

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
   [:supports-search? :boolean]
   [:capabilities [:vector :keyword]]])

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

(def capabilities
  "What callers may rely on beyond the port. :embed-text: an entry's transient
   :embed-text is embedded in place of its :content and never stored."
  [:embed-text])

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
                              (supports-semantic-search? config-atom))
       :capabilities capabilities})))

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