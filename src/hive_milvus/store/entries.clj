(ns hive-milvus.store.entries
  "Core entry CRUD, query, search, expiry, and status helpers."
  (:require [hive-milvus.store.deref :as d]
            [hive-milvus.embed.port :as port]
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
            [malli.core :as m]
            [hive-milvus.failure :as failure]))

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
      (d/deref! :add (milvus/add coll-name [record] :upsert? true))
      (:id record))))

(def ReadFailure
  "One collection that could not answer a read."
  [:map {:closed true}
   [:collection :string]
   [:message [:maybe :string]]])

(def WriteFailure
  "One collection that did not take a write (delete)."
  [:map {:closed true}
   [:collection :string]
   [:message [:maybe :string]]])

(defn- collection-failure
  "A ReadFailure / WriteFailure for `coll`, from the exception it raised."
  [coll ^Throwable e]
  {:collection coll :message (or (ex-message e) (str (class e)))})

(defn- fold-collections
  "Run STEP `(fn [coll] value-or-nil)` over `colls` in order, stopping at the
   first non-nil value when `stop-on-hit?`. A collection that does not exist
   (`lookup/missing-collection?`) answered: it holds nothing. Any other
   exception is a failure. Returns `{:found v :failed [..] :cause e}`."
  [step colls stop-on-hit?]
  (reduce (fn [acc coll]
            (try
              (let [v (step coll)]
                (if (and stop-on-hit? (some? v))
                  (reduced (assoc acc :found v))
                  acc))
              (catch Exception e
                (if (lookup/missing-collection? e)
                  acc
                  (-> acc
                      (update :failed conj (collection-failure coll e))
                      (update :cause #(or % e)))))))
          {:failed []}
          colls))

(defn locate-entry
  "Ask each of `colls`, in order, for `id` through FETCH
   (`(fn [coll id] value-or-nil)`), stopping at the first value.

     r/ok value   some collection answered with it
     r/ok nil     EVERY collection answered and none holds `id` - absence.
                  A configured collection that was never created counts as
                  answered (`lookup/missing-collection?`): it holds nothing.
     r/err :milvus/read-incomplete {:id :failed [ReadFailure ...] :cause e}
                  no value was found AND at least one collection failed, so
                  absence cannot be told from an outage

   A hit wins over a failing neighbour. Never throws: the decision is data,
   and `located-value` is the boundary that raises it."
  [fetch colls id]
  (let [{:keys [found failed cause]} (fold-collections #(fetch % id) colls true)]
    (cond
      (some? found) (r/ok found)
      (seq failed)  (r/err :milvus/read-incomplete
                           {:id id :failed failed :cause cause})
      :else         (r/ok nil))))

(m/=> locate-entry [:=> [:cat fn? [:sequential :string] :any] :map])

(defn located-value
  "The value a `locate-entry` result answers to a caller: the entry, nil for
   a true absence, or a raised ex-info for an incomplete read - never nil for
   a failure. ex-data carries `:hive-milvus/failed-collections`, the key
   `query-entries` uses.

   The ex-info wraps the first failing collection's exception as its cause,
   so `resilient` classifies it by that cause: a TRANSIENT cause is retried
   once and, if still failing, answered as the legacy
   `{:success? false ...}` map; a FATAL cause is re-thrown by `resilient`
   (`retry/classify-err`), so the caller sees this ex-info."
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
   holds it (a configured collection that was never created answers 'none').

   A collection that cannot be read is NOT absence: when no collection holds
   `id` and one failed, the read raises inside `resilient` (see
   `located-value`). A transient cause is retried once, then answered as
   `{:success? false :errors [..] :reconnecting? true}`; a fatal cause
   propagates as the ex-info. Either way, never nil."
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
  (let [rows (d/deref! :query-scalar (milvus/query-scalar coll
                {:filter            (str "id == \"" id "\"")
                 :limit             1
                 :output-fields     ["id" "embedding" "type" "tags" "content"
                                     "content_hash" "created" "updated"
                                     "duration" "expires"
                                     "access_count" "helpful_count" "unhelpful_count"
                                     "project_id"]
                 :consistency-level :strong}))]
    (when-let [row (first rows)]
      (let [existing  (schema/record->entry row)
            embedding (:embedding row)
            merged    (-> existing
                          (merge updates)
                          (assoc :id id :updated (schema/now-iso)))
            record    (schema/entry->record-pure merged coll embedding)]
        (d/deref! :add (milvus/add coll [record] :upsert? true))
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
   answered and none holds `id`. A collection that failed is not absence
   (see `locate-entry` / `located-value`): a transient cause reaches the
   caller as the legacy failure map from `resilient`, a fatal cause as the
   raised ex-info. Never a nil that reads as 'unknown id'."
  [config-atom id updates]
  (resilient config-atom
    (located-value
     (locate-entry (partial merge-keep-embedding! updates)
                   (lookup/known-collections config-atom)
                   id))))

(defn update-failure
  "The legacy error map `update-entry!` answers for a relocate-pipeline
   r/err `res` on `id`:

     {:error category :id id :detail (dissoc res :error) :reconnecting? bool}

   `:reconnecting?` says whether a retry can change the answer, the same
   flag `resilient`'s failure map carries, so a queue drain re-queues only
   transient failures. Only a `:boundary/*` effect error (a Milvus call that
   threw) can be transient, and only when its message is a transport drop or
   timeout (`failure/classify`). Routing, embedding and collector errors are
   permanent. Pure."
  [id res]
  (let [category (:error res)
        transient? (boolean
                    (and (keyword? category)
                         (= "boundary" (namespace category))
                         (when-let [msg (:message res)]
                           (or (re-find #"(?i)timed out" msg)
                               (failure/transient?
                                (failure/classify (ex-info msg {})))))))]
    {:error         category
     :id            id
     :detail        (dissoc res :error)
     :reconnecting? transient?}))

(defn update-result
  "The hive-spi write-contract value (`hive-spi.memory.contract/UpdateResult`)
   for a `relocate-update` result `res` on `id`. Pure:

     r/ok merged             -> the merged entry, carrying `id` as its :id
                                (an :id inside the updates cannot rename it)
     r/err :collector/not-found -> nil, the id is absent
     any other r/err         -> the `update-failure` map (:error non-nil)"
  [id res]
  (cond
    (r/ok? res)                            (assoc (:ok res) :id id)
    (= :collector/not-found (:error res))  nil
    :else                                  (update-failure id res)))

(defn update-entry!
  "Update an entry's fields. Routing-aware via the CPPB-layered
   pipeline — when the merged entry's target collection differs from
   its current collection, the pipeline relocates it transparently.

   Delegates to `hive-milvus.relocate.pipeline/relocate-update` and answers
   its result through `update-result`: the merged entry (carrying `id`) on
   success, nil when `id` is unknown, or the `update-failure` map
   (`{:error .. :reconnecting? transient?}`) for downstream errors - each a
   value of hive-spi's UpdateResult contract.

   Migration path: callers that want railway-tracked errors should
   call `reloc-pipeline/relocate-update` directly instead of this
   facade."
  [config-atom id updates]
  (update-result id (reloc-pipeline/relocate-update config-atom id updates)))

(defn delete-everywhere
  "Delete `id` from every one of `colls` through DELETE (`(fn [coll id])`).

     r/ok true    every collection took the delete (an unknown id included;
                  a collection that was never created holds nothing to
                  delete, see `lookup/missing-collection?`)
     r/err :milvus/write-incomplete {:id :failed [WriteFailure ...] :cause e}
                  some collection did not, so the entry may survive there

   Every collection is attempted even after a failure. Never throws."
  [delete colls id]
  (let [{:keys [failed cause]} (fold-collections #(delete % id) colls false)]
    (if (seq failed)
      (r/err :milvus/write-incomplete {:id id :failed failed :cause cause})
      (r/ok true))))

(m/=> delete-everywhere [:=> [:cat fn? [:sequential :string] :any] :map])

(defn deleted-value
  "The value a `delete-everywhere` result answers: true, or a raised ex-info
   when some collection did not take the delete. Like `located-value`, the
   ex-info wraps the first failing collection's exception as its cause (so
   `resilient` classifies it) and carries `:hive-milvus/failed-collections`."
  [res]
  (if (r/ok? res)
    (:ok res)
    (throw (ex-info (str "milvus delete of " (pr-str (:id res)) " incomplete: "
                         (count (:failed res)) " collection(s) did not take it,"
                         " the entry may survive there")
                    {:hive-milvus/failed-collections (:failed res)
                     :id (:id res)}
                    (:cause res)))))

(defn delete-entry!
  "Delete `id` from every known collection. true when every collection took
   the delete (one that was never created has nothing to take). A collection
   that failed means the delete did not land, so it raises inside
   `resilient` (`deleted-value`): a transient cause is retried once and then
   answered as the hive-spi Failure map (`{:success? false ...}`), a fatal
   cause propagates as the ex-info. Never a `true` for a lost delete."
  [config-atom id]
  (resilient config-atom
    (deleted-value
     (delete-everywhere (fn [coll id] (d/deref! :delete (milvus/delete coll [id])))
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
                           (d/deref! :query-scalar (milvus/query-scalar coll-name
                              {:filter filt :limit page-limit
                               :output-fields fields
                               :consistency-level :bounded})))
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
           (let [expired (d/deref! :query-scalar (milvus/query-scalar coll-name
                            {:filter (str "expires != \"\" and expires < \"" now "\"")
                             :output-fields ["id"]
                             :limit 10000}))
                 ids     (into [] (comp (map :id) (remove protected)) expired)
                 spared  (- (count expired) (count ids))]
             (when (seq ids)
               (d/deref! :delete (milvus/delete coll-name ids))
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
                 (d/deref! :query-scalar (milvus/query-scalar coll-name
                    {:filter (str/join " and " filter-cls)
                     :output-fields schema/default-read-fields
                     :limit 1000})))
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
                (when-let [hit (first (d/deref! :query-scalar (milvus/query-scalar coll-name
                                         {:filter filter-expr
                                          :output-fields schema/default-read-fields
                                          :limit 1})))]
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
  (let [row (first (d/deref! :query-scalar (milvus/query-scalar
                     collection-name
                     {:filter "id != \"\""
                      :output-fields ["count(*)"]
                      :limit 1})))
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
        (when (d/deref! :has-collection (milvus/has-collection coll-name))
          (d/deref! :drop-collection (milvus/drop-collection coll-name))
          (index/invalidate-loaded-collection! coll-name))
        (catch Exception _ nil)))
    true))