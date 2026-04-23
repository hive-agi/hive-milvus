(ns hive-milvus.store
  "Milvus implementation of IMemoryStore protocol.

   Standalone addon project — hive-mcp depends on this via IAddon,
   never the reverse. Wraps milvus-clj.api into protocol methods.

   Key difference from Chroma: Milvus requires explicit embeddings on
   insert and search. We use hive-mcp.embeddings.service to generate
   them — the same provider chain as Chroma.

   DDD: Repository pattern — MilvusMemoryStore is the Milvus aggregate adapter.

   This namespace is the façade over four cohesive submodules:
     hive-milvus.store.schema  — entry<->record, filter expressions
     hive-milvus.store.index   — collection loading + scalar indexes
     hive-milvus.store.query   — single-entry read / read-modify-write
     hive-milvus.store.health  — reconnect loop, resilient retry, liveness

   The `defrecord MilvusMemoryStore` + `create-store` live here because
   the protocol methods can't be split across files."
  (:require [hive-mcp.protocols.memory :as proto]
            [hive-mcp.embeddings.service :as embed-svc]
            [hive-milvus.store.schema :as schema]
            [hive-milvus.store.index :as index]
            [hive-milvus.store.query :as query]
            [hive-milvus.store.health :as health :refer [resilient]]
            [milvus-clj.api :as milvus]
            [clojure.string :as str]
            [taoensso.timbre :as log]))

;; =========================================================================
;; Re-exports — preserve hive-milvus.store/* API for the existing test
;; suite after the schema|index|query|health split. Pure façade aliases,
;; no new behaviour. Tests that poke deeper internals (health-cache,
;; ensure-live!, etc.) now reference hive-milvus.store.health directly.
;; =========================================================================

(def tags->str              schema/tags->str)
(def str->tags              schema/str->tags)
(def record->entry          schema/record->entry)
(def entry->record          schema/entry->record)
(def build-filter-expr      schema/build-filter-expr)
(def staleness-probability  schema/staleness-probability)
(def helpfulness-map        schema/helpfulness-map)
(def with-auto-reconnect    health/with-auto-reconnect)

;; =========================================================================
;; Protocol Implementation
;; =========================================================================

(defrecord MilvusMemoryStore [config-atom]
  ;; =========================================================================
  ;; IMemoryStore - Core Protocol
  ;; =========================================================================
  proto/IMemoryStore

  ;; --- Connection Lifecycle ---

  (connect! [_this config]
    ;; Keepalive defaults tuned for fragile intermediaries (Tailscale
    ;; userspace netstack, cloud NAT): ping every 10s < gVisor idle-flow
    ;; GC window, without-calls? forces pings on idle clients. Foreground
    ;; retries are bounded — after the budget the background heal loop
    ;; takes over and `with-auto-reconnect` transparently sees recovery.
    (let [milvus-config (into {} (filter (comp some? val))
                              (select-keys config [:transport
                                                   :host :port :token :database :secure
                                                   :connect-timeout-ms
                                                   :keep-alive-time-ms
                                                   :keep-alive-timeout-ms
                                                   :keep-alive-without-calls?
                                                   :idle-timeout-ms]))
          milvus-config (merge {:connect-timeout-ms        30000
                                :keep-alive-time-ms        10000
                                :keep-alive-timeout-ms     20000
                                :keep-alive-without-calls? true}
                               milvus-config)
          coll-name     (or (:collection-name config) "hive_mcp_memory")
          max-retries   6
          base-ms       1000
          max-ms        15000]
      (swap! config-atom merge config)
      (loop [attempt 1]
        (let [result
              (try
                (milvus/connect! milvus-config)
                (let [dimension (embed-svc/get-dimension-for coll-name)]
                  (index/ensure-collection! coll-name dimension))
                {:success? true
                 :backend  "milvus"
                 :errors   []
                 :metadata (select-keys @config-atom [:host :port :collection-name])}
                (catch Exception e
                  {:success? false
                   :backend  "milvus"
                   :errors   [(.getMessage e)]
                   :metadata {}}))]
          (cond
            (:success? result)
            result

            (>= attempt max-retries)
            (do
              (log/warn "Milvus connect failed after" max-retries
                        "foreground attempts; handing off to background healing loop")
              (when-not (:running? @health/reconnect-state)
                (health/start-reconnect-loop! config-atom))
              (assoc result :reconnecting? true))

            :else
            (let [wait (health/backoff-ms attempt base-ms max-ms)]
              (log/info "Milvus connect attempt" attempt "failed, retrying in" (/ wait 1000.0) "s")
              (Thread/sleep wait)
              (recur (inc attempt))))))))

  (disconnect! [_this]
    (try
      (milvus/disconnect!)
      (reset! index/loaded-collections #{})
      {:success? true :errors []}
      (catch Exception e
        {:success? false :errors [(.getMessage e)]})))

  (connected? [_this]
    (milvus/connected?))

  (health-check [_this]
    (let [start-ms (System/currentTimeMillis)
          ts       (schema/now-iso)]
      (try
        (let [coll-name (:collection-name @config-atom "hive_mcp_memory")
              exists?   @(milvus/has-collection coll-name)
              latency   (- (System/currentTimeMillis) start-ms)]
          {:healthy?    exists?
           :latency-ms  latency
           :backend     "milvus"
           :entry-count nil
           :errors      (if exists? [] ["Collection does not exist"])
           :checked-at  ts})
        (catch Exception e
          {:healthy?    false
           :latency-ms  (- (System/currentTimeMillis) start-ms)
           :backend     "milvus"
           :entry-count nil
           :errors      [(.getMessage e)]
           :checked-at  ts}))))

  ;; --- CRUD Operations ---

  (add-entry! [_this entry]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")
            record    (schema/entry->record entry coll-name)]
        @(milvus/add coll-name [record] :upsert? true)
        (:id record))))

  (get-entry [_this id]
    (resilient config-atom
      (query/get-entry-by-id (:collection-name @config-atom "hive_mcp_memory") id)))

  (update-entry! [_this id updates]
    (resilient config-atom
      (query/update-entry-fields! (:collection-name @config-atom "hive_mcp_memory")
                                  id updates)))

  (delete-entry! [_this id]
    (resilient config-atom
      @(milvus/delete (:collection-name @config-atom "hive_mcp_memory") [id])
      true))

  (query-entries [_this opts]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")
            _         (index/ensure-scalar-indexes! coll-name) ;; lazy belt: idempotent, no-ops when memoized
            {:keys [limit output-fields]
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
        (mapv schema/record->entry results))))

  ;; --- Semantic Search ---

  (search-similar [_this query-text opts]
    (resilient config-atom
      (let [coll-name   (:collection-name @config-atom "hive_mcp_memory")
            {:keys [limit type project-ids exclude-tags]
             :or   {limit 10}} opts
            query-vec   (embed-svc/embed-for-collection coll-name query-text)
            filter-expr (schema/build-filter-expr
                          (cond-> {:include-expired? false}
                            type        (assoc :type type)
                            project-ids (assoc :project-ids project-ids)
                            exclude-tags (assoc :exclude-tags exclude-tags)))
            results     @(milvus/query coll-name
                           (cond-> {:vector query-vec :limit limit
                                    :output-fields schema/default-read-fields}
                             filter-expr (assoc :filter filter-expr)))]
        (mapv schema/record->entry results))))

  (supports-semantic-search? [_this]
    (embed-svc/provider-available-for?
      (:collection-name @config-atom "hive_mcp_memory")))

  ;; --- Expiration Management ---

  (cleanup-expired! [_this]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")
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

  (entries-expiring-soon [_this days opts]
    (resilient config-atom
      (let [coll-name  (:collection-name @config-atom "hive_mcp_memory")
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

  ;; --- Duplicate Detection ---

  (find-duplicate [_this type content-hash opts]
    (resilient config-atom
      (let [coll-name  (:collection-name @config-atom "hive_mcp_memory")
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

  ;; --- Store Management ---

  (store-status [_this]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")]
        {:backend          "milvus"
         :configured?      (milvus/connected?)
         :entry-count      (try
                             (count @(milvus/query-scalar coll-name
                                       {:filter "id != \"\""
                                        :output-fields ["id"]
                                        :limit 100000}))
                             (catch Exception _ nil))
         :supports-search? (milvus/connected?)})))

  (reset-store! [_this]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")]
        (when @(milvus/has-collection coll-name)
          @(milvus/drop-collection coll-name))
        (index/invalidate-loaded-collection! coll-name)
        true)))

  ;; =========================================================================
  ;; IMemoryStoreWithAnalytics
  ;; =========================================================================
  proto/IMemoryStoreWithAnalytics

  (log-access! [_this id]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")]
        (when-let [entry (query/get-entry-by-id coll-name id)]
          (query/update-entry-fields! coll-name id
            {:access-count (inc (or (:access-count entry) 0))})))))

  (record-feedback! [_this id feedback]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")]
        (when-let [entry (query/get-entry-by-id coll-name id)]
          (let [field     (schema/feedback->field feedback)
                new-count (inc (or (get entry field) 0))]
            (query/update-entry-fields! coll-name id {field new-count}))))))

  (get-helpfulness-ratio [_this id]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")]
        (when-let [entry (query/get-entry-by-id coll-name id)]
          (schema/helpfulness-map entry)))))

  ;; =========================================================================
  ;; IMemoryStoreWithStaleness
  ;; =========================================================================
  proto/IMemoryStoreWithStaleness

  (update-staleness! [_this id staleness-opts]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")
            {:keys [beta source depth]} staleness-opts]
        (query/update-entry-fields! coll-name id
          (cond-> {}
            beta   (assoc :staleness-beta beta)
            source (assoc :staleness-source (name source))
            depth  (assoc :staleness-depth depth))))))

  (get-stale-entries [_this threshold opts]
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

  (propagate-staleness! [_this source-id depth]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")]
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

  ;; =========================================================================
  ;; IMemoryStoreBatch — single-RPC multi-ID fetch (catchup hot path)
  ;; =========================================================================
  proto/IMemoryStoreBatch

  (get-entries [_this ids]
    (let [ids (vec (distinct (remove nil? ids)))]
      (when (seq ids)
        (resilient config-atom
          (let [coll-name (:collection-name @config-atom "hive_mcp_memory")
                results   @(milvus/get coll-name ids
                             :consistency-level :bounded
                             :include schema/default-read-fields)]
            (mapv schema/record->entry results)))))))

(defn create-store
  "Create a new Milvus-backed memory store.

   Options (also configurable via connect!):
     :host            - Milvus gRPC host (default: localhost)
     :port            - Milvus gRPC port (default: 19530)
     :collection-name - Collection name (default: hive_mcp_memory)

   Returns an IMemoryStore implementation.

   Example:
     (def store (create-store {:host \"milvus.milvus.svc\" :port 19530}))
     (proto/connect! store {:host \"milvus.milvus.svc\" :port 19530})
     (proto/set-store! store)"
  ([]
   (create-store {}))
  ([opts]
   (log/info "Creating MilvusMemoryStore" (when (seq opts) opts))
   (->MilvusMemoryStore (atom (merge {:host "localhost"
                                       :port 19530
                                       :collection-name "hive_mcp_memory"}
                                      opts)))))
