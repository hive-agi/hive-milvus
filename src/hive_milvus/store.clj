(ns hive-milvus.store
  "Milvus implementation of IMemoryStore protocol.

   Standalone addon project — hive-mcp depends on this via IAddon,
   never the reverse. Wraps milvus-clj.api into protocol methods.

   Key difference from Chroma: Milvus requires explicit embeddings on
   insert and search. We use hive-mcp.embeddings.service to generate
   them — the same provider chain as Chroma.

   DDD: Repository pattern — MilvusMemoryStore is the Milvus aggregate adapter."
  (:require [hive-mcp.protocols.memory :as proto]
            [hive-mcp.embeddings.service :as embed-svc]
            [hive-mcp.dns.result :as r]
            [hive-dsl.result :as dsl-r]
            [hive-milvus.circuit :as circuit]
            [hive-milvus.failure :as failure]
            [milvus-clj.api :as milvus]
            [milvus-clj.schema :as milvus-schema]
            [milvus-clj.index :as milvus-index]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [taoensso.timbre :as log]))

;; =========================================================================
;; Pure Calculations
;; =========================================================================

(defn- now-iso
  "Current ISO 8601 timestamp string."
  []
  (str (java.time.ZonedDateTime/now (java.time.ZoneId/systemDefault))))

(defn staleness-probability
  "Calculate staleness probability from alpha/beta parameters."
  [entry]
  (let [alpha (or (:staleness-alpha entry) 1)
        beta  (or (:staleness-beta entry) 1)]
    (/ (double beta) (+ alpha beta))))

(defn- feedback->field
  "Map feedback keyword to entry field name."
  [feedback]
  (case feedback
    :helpful   :helpful-count
    :unhelpful :unhelpful-count))

(defn helpfulness-map
  "Build helpfulness ratio map from entry counts."
  [entry]
  (let [helpful   (or (:helpful-count entry) 0)
        unhelpful (or (:unhelpful-count entry) 0)
        total     (+ helpful unhelpful)]
    {:helpful-count   helpful
     :unhelpful-count unhelpful
     :total           total
     :ratio           (if (pos? total) (double (/ helpful total)) 0.0)}))

;; =========================================================================
;; Entry <-> Milvus Record Conversion
;; =========================================================================

(defn tags->str
  "Serialize tags to JSON string for Milvus VarChar field."
  [tags]
  (if (sequential? tags)
    (json/write-str tags)
    (or (str tags) "[]")))

(defn str->tags
  "Deserialize tags from JSON string."
  [s]
  (when (and s (not (str/blank? s)))
    (try (json/read-str s :key-fn keyword)
         (catch Exception _ []))))

(defn- entry->record
  "Convert a memory entry map to a Milvus insert record.
   Generates embedding via the embedding service."
  [entry collection-name]
  (let [raw-content (or (:content entry) "")
        content     (if (map? raw-content) (json/write-str raw-content) (str raw-content))
        embedding   (embed-svc/embed-for-collection collection-name content)]
    {:id              (or (:id entry) (proto/generate-id))
     :embedding       embedding
     :document        content
     :type            (or (some-> (:type entry) name) "note")
     :tags            (tags->str (:tags entry))
     :content         content
     :content_hash    (or (:content-hash entry) "")
     :created         (or (:created entry) (now-iso))
     :updated         (or (:updated entry) (now-iso))
     :duration        (or (some-> (:duration entry) name) "medium")
     :expires         (or (:expires entry) "")
     :access_count    (or (:access-count entry) 0)
     :helpful_count   (or (:helpful-count entry) 0)
     :unhelpful_count (or (:unhelpful-count entry) 0)
     :project_id      (or (:project-id entry) "")}))

(defn- try-parse-json
  "Parse JSON string to map/vec, return original on failure."
  [s]
  (if (and (string? s)
           (or (str/starts-with? s "{") (str/starts-with? s "[")))
    (r/rescue s (json/read-str s :key-fn keyword))
    s))

(defn record->entry
  "Convert a Milvus query result row to a memory entry map."
  [row]
  (let [tags-raw (:tags row)
        raw-content (or (:content row) (:document row) "")]
    (cond-> {:id              (:id row)
             :type            (keyword (or (:type row) "note"))
             :content         (try-parse-json raw-content)
             :tags            (str->tags tags-raw)
             :content-hash    (:content_hash row)
             :created         (:created row)
             :updated         (:updated row)
             :duration        (keyword (or (:duration row) "medium"))
             :expires         (when-let [e (:expires row)]
                                (when-not (str/blank? e) e))
             :access-count    (or (:access_count row) 0)
             :helpful-count   (or (:helpful_count row) 0)
             :unhelpful-count (or (:unhelpful_count row) 0)
             :project-id      (let [pid (:project_id row)]
                                (when-not (str/blank? pid) pid))}
      (:distance row) (assoc :distance (:distance row)))))

;; =========================================================================
;; Filter Expression Builders
;; =========================================================================

(defn build-filter-expr
  "Build a Milvus boolean filter expression from query opts."
  [{:keys [type project-id project-ids tags exclude-tags include-expired?]}]
  (let [clauses (cond-> []
                  type
                  (conj (str "type == \"" (name type) "\""))

                  project-id
                  (conj (str "project_id == \"" project-id "\""))

                  (and project-ids (seq project-ids))
                  (conj (str "project_id in ["
                             (str/join ", " (map #(str "\"" % "\"") project-ids))
                             "]"))

                  (and tags (seq tags))
                  (into (map (fn [t] (str "tags like \"%" t "%\"")) tags))

                  (and exclude-tags (seq exclude-tags))
                  (into (map (fn [t] (str "not (tags like \"%" t "%\")")) exclude-tags))

                  (not include-expired?)
                  (conj (str "(expires == \"\" or expires > \""
                             (now-iso) "\")")))]
    (when (seq clauses)
      (str/join " and " clauses))))

;; =========================================================================
;; Milvus Helpers
;; =========================================================================

(defonce ^:private loaded-collections
  ;; Per-process memo of collections we've already `load-collection`'d.
  ;; `load-collection` is expensive (seconds) while subsequent queries are
  ;; cheap — calling it on every `connect!` regresses cold paths badly.
  ;; Invalidated by `invalidate-loaded-collection!` when reconnect detects
  ;; a fresh server or collection was dropped.
  (atom #{}))

(defn- invalidate-loaded-collection!
  [collection-name]
  (swap! loaded-collections disj collection-name))

(defn- ensure-collection!
  "Ensure the memory collection exists and is loaded into memory.
   After Milvus restart, collections exist but aren't loaded — queries
   return empty unless we explicitly load. We memoize the load per
   process so repeated `connect!` calls don't re-issue the expensive
   load RPC."
  [collection-name dimension]
  (if-not @(milvus/has-collection collection-name)
    (do (log/info "Creating Milvus collection:" collection-name "dim:" dimension)
        @(milvus/create-collection collection-name
           {:schema (milvus-schema/with-dimension dimension)
            :index  milvus-index/default-memory-index
            :description "hive-mcp memory store"})
        (swap! loaded-collections conj collection-name))
    (if (contains? @loaded-collections collection-name)
      (log/debug "Collection already loaded in this process, skipping:" collection-name)
      (do (log/info "Loading Milvus collection into memory:" collection-name)
          (try
            @(milvus/load-collection collection-name)
            (swap! loaded-collections conj collection-name)
            (catch Exception e
              (log/debug "load-collection (may already be loaded):" (.getMessage e))
              (swap! loaded-collections conj collection-name)))))))

(defn- get-entry-by-id
  "Fetch a single entry by ID from Milvus. Returns entry map or nil.
   Uses STRONG consistency to ensure read-after-write visibility."
  [collection-name id]
  (let [results @(milvus/get collection-name [id] :consistency-level :strong)]
    (when (seq results)
      (record->entry (first results)))))

(defn- update-entry-fields!
  "Update an entry by reading, merging, and upserting."
  [collection-name id updates]
  (when-let [existing (get-entry-by-id collection-name id)]
    (let [merged (merge existing updates)
          record (entry->record (assoc merged :id id :updated (now-iso))
                                collection-name)]
      @(milvus/add collection-name [record] :upsert? true)
      merged)))

;; =========================================================================
;; Resilient Connection — auto-heal on failure
;; =========================================================================

(def ^:private reconnect-state
  "Tracks background reconnect loop. Keys: :running? :future :last-attempt"
  (atom {:running? false :future nil :last-attempt nil}))

(defn- backoff-ms
  "Exponential backoff with jitter. attempt starts at 1.
   base-ms doubles each attempt, capped at max-ms, plus ±25% jitter so
   multiple clients don't synchronise their retry storms."
  [attempt base-ms max-ms]
  (let [raw     (* base-ms (bit-shift-left 1 (min 20 (dec attempt))))
        capped  (min (long raw) (long max-ms))
        jitter  (long (* capped 0.25 (- (rand) 0.5) 2))]
    (max 100 (+ capped jitter))))

(defn- try-reconnect!
  "Single reconnect attempt using stored config. Returns true on success.

   Carries `:transport` through so the reactive heal loop reconnects
   with whichever transport the store was originally configured with —
   essential for the hybrid-transport story (PR-3): the same heal
   machinery serves both `:grpc` (rebuild the channel) and `:http`
   (idempotent re-init of the JDK HttpClient)."
  [config-atom]
  (try
    (let [cfg @config-atom
          milvus-cfg (into {} (filter (comp some? val))
                           (select-keys cfg [:transport :host :port :token
                                             :database :secure]))
          milvus-cfg (merge {:connect-timeout-ms 30000} milvus-cfg)]
      (milvus/connect! milvus-cfg)
      (log/info "Milvus reconnected successfully")
      true)
    (catch Exception e
      (log/debug "Milvus reconnect attempt failed:" (.getMessage e))
      false)))

(defn- start-reconnect-loop!
  "Background healing loop: retries forever with exponential backoff + jitter,
   capped at `max-ms`. Runs until the store is reached and `try-reconnect!`
   succeeds, or until another caller flips `:running?` back to false (e.g.
   `disconnect!`). No attempt ceiling — intermittent outages should self-heal
   whenever the network comes back."
  [config-atom & {:keys [base-ms max-ms]
                  :or   {base-ms 1000 max-ms 60000}}]
  (when (compare-and-set! reconnect-state
          (assoc @reconnect-state :running? false)
          (assoc @reconnect-state :running? true))
    (let [fut (future
                (loop [n 1]
                  (when (:running? @reconnect-state)
                    (swap! reconnect-state assoc :last-attempt (System/currentTimeMillis))
                    (if (try-reconnect! config-atom)
                      (do
                        (log/info "Milvus reconnect loop recovered after" n "attempts")
                        (swap! reconnect-state assoc :running? false))
                      (let [wait (backoff-ms n base-ms max-ms)]
                        (log/debug "Milvus reconnect attempt" n "failed, next in" (/ wait 1000.0) "s")
                        (Thread/sleep wait)
                        (recur (inc n)))))))]
      (swap! reconnect-state assoc :future fut))))

(defn- await-reconnect!
  "Block up to `max-wait-ms` waiting for the background reconnect loop
   to recover the milvus connection. Returns true if the connection is
   up at the end of the wait (reconnect succeeded or we lucked out),
   false if the deadline passed with the loop still running."
  [max-wait-ms]
  (let [deadline (+ (System/currentTimeMillis) max-wait-ms)]
    (loop []
      (cond
        (milvus/connected?)                       true
        (>= (System/currentTimeMillis) deadline)  (milvus/connected?)
        :else (do (Thread/sleep 200) (recur))))))

(defn- kick-reconnect!
  "Drop the dead milvus client and start the background heal loop if
   it isn't already running. Idempotent. Extracted so the reactive path
   and test hooks can call it without duplicating the disconnect + CAS."
  [config-atom]
  ;; Explicitly drop the dead client so `milvus/connected?` reports
  ;; false until the background loop installs a fresh one. Without
  ;; this, `await-reconnect!` returns immediately because the atom
  ;; still holds the broken client.
  (try (milvus/disconnect!) (catch Throwable _ nil))
  (when-not (:running? @reconnect-state)
    (log/info "Starting background reconnect loop")
    (start-reconnect-loop! config-atom)))

(defn- attempt-call
  "Run `f` under a try-effect* that tags any Throwable as a raw
   :milvus/call error carrying the original exception. No classification
   yet — classification happens in `r/map-err` so the pipeline stays
   linear."
  [f]
  (try (dsl-r/ok (f))
       (catch Throwable e
         (dsl-r/err :milvus/call {:throwable e}))))

(defn- classify-err
  "Called by `map-err`: lift the raw `{:throwable e}` error into a
   MilvusFailure ADT. Re-throws non-connection failures so callers
   outside the resilient retry path keep seeing exceptions for genuine
   programming errors (the original contract of with-auto-reconnect)."
  [err]
  (let [t       (:throwable err)
        failure (failure/classify t)]
    (if (failure/transient? failure)
      (dsl-r/err :milvus/failure {:failure failure})
      ;; Fatal: re-throw so host code sees the real stack trace.
      (throw t))))

(defn- retry-once
  "Second-chance step for the pipeline. On a transient failure, kick
   the background heal loop, block up to `budget-ms` for recovery, then
   retry `f` one time. On recovery failure or second-attempt failure we
   carry the failure forward through the Result chain. Never throws for
   transient paths — fatal exceptions already unwound in `classify-err`."
  [config-atom f budget-ms]
  (fn [err]
    (let [failure (:failure err)
          msg     (:message failure)]
      (circuit/record-failure!)
      (log/warn "Milvus operation failed (transient):" msg)
      (kick-reconnect! config-atom)
      (if (await-reconnect! budget-ms)
        ;; Recovery path: retry exactly once.
        (let [retried (attempt-call f)]
          (if (dsl-r/ok? retried)
            (do (circuit/record-success!) retried)
            (dsl-r/err :milvus/failure
                       {:failure (failure/classify
                                   (:throwable retried))})))
        ;; Budget exhausted before reconnect completed.
        (do (log/warn "Milvus reconnect did not complete within budget"
                      budget-ms "ms")
            (dsl-r/err :milvus/failure
                       {:failure (failure/reconnect-timeout msg)}))))))

(defn- with-auto-reconnect
  "Execute `f` with transparent reconnect-and-retry on transient gRPC/
   connection failures. Flat `hive-dsl.result` pipeline:

     attempt -> classify -> (maybe) retry -> legacy-shape

   Semantics preserved from the old nested-try/catch implementation:

   - Circuit breaker gated first: if :open and in cooldown we return the
     breaker's fail map without running `f`.
   - Fatal exceptions (non-transient) are re-thrown — callers outside
     this helper keep their existing contract.
   - Transient failures kick the background reconnect loop, block up to
     `budget-ms` for recovery, then retry `f` exactly once.
   - Final failure (second attempt, or reconnect timeout) is translated
     back to the legacy `{:success? false :errors [...] :reconnecting? ...}`
     map so protocol callers continue to see the shape they expect.

   Why inline reactive retry: tailscale userspace netstack (and any
   NAT-style intermediary) can drop an idle gRPC flow between RPCs.
   Returning the raw failure to the caller forces every caller to
   implement retry, which they don't."
  ([config-atom f]
   (with-auto-reconnect config-atom f 8000))
  ([config-atom f budget-ms]
   (if-let [blocked (circuit/check)]
     blocked
     (let [result (-> (attempt-call f)
                      (dsl-r/map-err classify-err)
                      (dsl-r/bind (fn [v]
                                    (circuit/record-success!)
                                    (dsl-r/ok v)))
                      (dsl-r/map-err (retry-once config-atom f budget-ms)))]
       (cond
         (dsl-r/ok? result)  (:ok result)
         (dsl-r/err? result) (failure/->legacy-map (:failure result))
         :else               result)))))

;; -------------------------------------------------------------------------
;; Preemptive liveness check (ensure-live!)
;; -------------------------------------------------------------------------
;;
;; Why: `with-auto-reconnect` is REACTIVE — the first post-idle RPC eats an
;; UNAVAILABLE, triggers the reconnect loop, then blocks up to 8s inside
;; await-reconnect! before retrying. `ensure-live!` is PROACTIVE — before
;; the RPC fires, check whether the client is alive; if not, kick the
;; background loop now so healing overlaps the caller's preparation latency.
;;
;; Cache: milvus/connected? is a cheap atom deref in milvus-clj, but we
;; still cache the result ~5s so a burst of RPCs doesn't repeatedly branch
;; on it. The cache is a process-wide atom — mirrors the singleton nature
;; of the milvus-clj client.

(def ^:private health-cache
  "Cached liveness snapshot. :ts = System/currentTimeMillis of last check,
   :alive? = milvus/connected? reading at that time."
  (atom {:ts 0 :alive? false}))

(def ^:private health-cache-ttl-ms
  "Positive liveness stays valid this long before re-verification.
   5s: short enough that a dropped connection is noticed before a
   typical multi-RPC request completes, long enough that a burst of
   RPCs amortises to a single check."
  5000)

(defn- kick-reconnect-now!
  "Preemptive sibling of the `kick-reconnect!` letfn inside
   `with-auto-reconnect`. Drops the dead client so milvus/connected?
   stops lying, then starts the background reconnect loop if not
   already running. Idempotent."
  [config-atom]
  (try (milvus/disconnect!) (catch Throwable _ nil))
  (when-not (:running? @reconnect-state)
    (log/info "ensure-live!: starting background reconnect loop preemptively")
    (start-reconnect-loop! config-atom)))

(defn- ensure-live!
  "Preemptive liveness gate. Called at the top of every `resilient` body.
   Reads a ~5s cached liveness snapshot; on cache miss re-verifies via
   milvus/connected?. If the client is dead OR the cache is stale (>5s)
   and the re-check returns false, kicks the background reconnect loop
   so healing starts before the caller's RPC fails.

   Does NOT block waiting for recovery — `with-auto-reconnect` still
   owns the reactive retry + await-reconnect! budget. This just shrinks
   the window between 'connection died' and 'reconnect loop running'.

   Returns true if the client is believed live at the end of the call,
   false if still dead (the caller's RPC will then hit the reactive path)."
  [config-atom]
  (let [now   (System/currentTimeMillis)
        cache @health-cache]
    (if (and (:alive? cache)
             (< (- now (:ts cache)) health-cache-ttl-ms))
      true
      (let [alive? (try (milvus/connected?) (catch Throwable _ false))]
        (reset! health-cache {:ts now :alive? alive?})
        (when-not alive?
          (kick-reconnect-now! config-atom))
        alive?))))

(defn- invalidate-health-cache!
  "Force the next ensure-live! to re-verify. Hook for the reactive
   retry path to call after observing a failure — keeps the preemptive
   cache honest once a reactive heal fires."
  []
  (swap! health-cache assoc :ts 0 :alive? false))

(defmacro resilient
  "Wrap `body` in `with-auto-reconnect`: on transient gRPC/connection
   failure (UNAVAILABLE, DEADLINE_EXCEEDED, \"Keepalive failed\", etc.)
   the background reconnect loop is kicked, we block briefly for it to
   install a fresh client, and `body` is re-executed once. Transparent
   to callers — they either see the successful retry result or a graceful
   {:success? false :reconnecting? true} map after the budget is spent.

   Before executing `body`, calls `ensure-live!` to proactively detect
   a dead client and start the reconnect loop early. The reactive
   with-auto-reconnect path still handles any failure that slips through.

   Intended for use inside MilvusMemoryStore protocol method bodies where
   `config-atom` is captured from the defrecord."
  [config-atom & body]
  `(do
     (ensure-live! ~config-atom)
     (with-auto-reconnect ~config-atom (fn [] ~@body))))

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
    (let [;; Don't overwrite env-var defaults with nil from config.
          ;; :transport opt is passed through (PR-3) so callers can pick
          ;; the underlying milvus-clj transport. Defaults to :grpc inside
          ;; the api.clj shim if omitted.
          milvus-config (into {} (filter (comp some? val))
                              (select-keys config [:transport
                                                   :host :port :token :database :secure
                                                   :connect-timeout-ms
                                                   :keep-alive-time-ms
                                                   :keep-alive-timeout-ms
                                                   :keep-alive-without-calls?
                                                   :idle-timeout-ms]))
          ;; Tuned for fragile intermediaries (Tailscale userspace netstack,
          ;; cloud NAT). Higher connect timeout for relay paths.
          ;; Keepalive pings tightened to 10 s in PR-3 (down from 30 s) —
          ;; below gVisor's idle-flow GC window. without-calls? is critical
          ;; — else gRPC only pings during active RPCs and idle clients die
          ;; silently with "UNAVAILABLE: Keepalive failed. The connection
          ;; is likely gone". Caller-supplied values in `config` win via
          ;; merge order. If even 10 s leaks, switch :transport to :http.
          milvus-config (merge {:connect-timeout-ms        30000
                                :keep-alive-time-ms        10000
                                :keep-alive-timeout-ms     20000
                                :keep-alive-without-calls? true}
                               milvus-config)
          coll-name     (or (:collection-name config) "hive_mcp_memory")
          ;; Foreground attempts are bounded so callers (MCP init, tests)
          ;; don't block indefinitely. After the budget is spent we hand
          ;; off to `start-reconnect-loop!` which heals forever in the
          ;; background; the next operation transparently sees success
          ;; via `with-auto-reconnect`.
          max-retries   6
          base-ms       1000
          max-ms        15000]
      (swap! config-atom merge config)
      (loop [attempt 1]
        (let [result
              (try
                (milvus/connect! milvus-config)
                (let [dimension (embed-svc/get-dimension-for coll-name)]
                  (ensure-collection! coll-name dimension))
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
              (when-not (:running? @reconnect-state)
                (start-reconnect-loop! config-atom))
              (assoc result :reconnecting? true))

            :else
            (let [wait (backoff-ms attempt base-ms max-ms)]
              (log/info "Milvus connect attempt" attempt "failed, retrying in" (/ wait 1000.0) "s")
              (Thread/sleep wait)
              (recur (inc attempt))))))))

  (disconnect! [_this]
    (try
      (milvus/disconnect!)
      (reset! loaded-collections #{})
      {:success? true :errors []}
      (catch Exception e
        {:success? false :errors [(.getMessage e)]})))

  (connected? [_this]
    (milvus/connected?))

  (health-check [_this]
    (let [start-ms (System/currentTimeMillis)
          ts       (now-iso)]
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
            record    (entry->record entry coll-name)]
        @(milvus/add coll-name [record] :upsert? true)
        (:id record))))

  (get-entry [_this id]
    (resilient config-atom
      (get-entry-by-id (:collection-name @config-atom "hive_mcp_memory") id)))

  (update-entry! [_this id updates]
    (resilient config-atom
      (update-entry-fields! (:collection-name @config-atom "hive_mcp_memory")
                            id updates)))

  (delete-entry! [_this id]
    (resilient config-atom
      @(milvus/delete (:collection-name @config-atom "hive_mcp_memory") [id])
      true))

  (query-entries [_this opts]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")
            {:keys [limit] :or {limit 100}} opts
            filter-expr (build-filter-expr opts)
            results (if filter-expr
                      @(milvus/query-scalar coll-name
                         {:filter filter-expr :limit limit
                          :consistency-level :strong})
                      @(milvus/query-scalar coll-name
                         {:filter "id != \"\"" :limit limit
                          :consistency-level :strong}))]
        (mapv record->entry results))))

  ;; --- Semantic Search ---

  (search-similar [_this query-text opts]
    (resilient config-atom
      (let [coll-name   (:collection-name @config-atom "hive_mcp_memory")
            {:keys [limit type project-ids exclude-tags]
             :or   {limit 10}} opts
            query-vec   (embed-svc/embed-for-collection coll-name query-text)
            filter-expr (build-filter-expr
                          (cond-> {:include-expired? false}
                            type        (assoc :type type)
                            project-ids (assoc :project-ids project-ids)
                            exclude-tags (assoc :exclude-tags exclude-tags)))
            results     @(milvus/query coll-name
                           (cond-> {:vector query-vec :limit limit}
                             filter-expr (assoc :filter filter-expr)))]
        (mapv record->entry results))))

  (supports-semantic-search? [_this]
    (embed-svc/provider-available-for?
      (:collection-name @config-atom "hive_mcp_memory")))

  ;; --- Expiration Management ---

  (cleanup-expired! [_this]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")
            now       (now-iso)
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
                           :limit 1000})]
        (mapv record->entry results))))

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
                           :limit 1})]
        (when (seq results)
          (record->entry (first results))))))

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
        (invalidate-loaded-collection! coll-name)
        true)))

  ;; =========================================================================
  ;; IMemoryStoreWithAnalytics
  ;; =========================================================================
  proto/IMemoryStoreWithAnalytics

  (log-access! [_this id]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")]
        (when-let [entry (get-entry-by-id coll-name id)]
          (update-entry-fields! coll-name id
            {:access-count (inc (or (:access-count entry) 0))})))))

  (record-feedback! [_this id feedback]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")]
        (when-let [entry (get-entry-by-id coll-name id)]
          (let [field     (feedback->field feedback)
                new-count (inc (or (get entry field) 0))]
            (update-entry-fields! coll-name id {field new-count}))))))

  (get-helpfulness-ratio [_this id]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")]
        (when-let [entry (get-entry-by-id coll-name id)]
          (helpfulness-map entry)))))

  ;; =========================================================================
  ;; IMemoryStoreWithStaleness
  ;; =========================================================================
  proto/IMemoryStoreWithStaleness

  (update-staleness! [_this id staleness-opts]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")
            {:keys [beta source depth]} staleness-opts]
        (update-entry-fields! coll-name id
          (cond-> {}
            beta   (assoc :staleness-beta beta)
            source (assoc :staleness-source (name source))
            depth  (assoc :staleness-depth depth))))))

  (get-stale-entries [_this threshold opts]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")
            {:keys [project-id type]} opts
            filter-expr (build-filter-expr {:project-id project-id :type type
                                            :include-expired? true})
            results @(milvus/query-scalar coll-name
                       {:filter (or filter-expr "id != \"\"")
                        :limit 10000})]
        (->> (mapv record->entry results)
             (filter #(> (staleness-probability %) threshold))
             vec))))

  (propagate-staleness! [_this source-id depth]
    (resilient config-atom
      (let [coll-name (:collection-name @config-atom "hive_mcp_memory")]
        (when-let [entry (get-entry-by-id coll-name source-id)]
          (let [kg-outgoing (:kg-outgoing entry)]
            (reduce (fn [cnt dep-id]
                      (if (and dep-id (seq dep-id))
                        (if-let [dep (get-entry-by-id coll-name dep-id)]
                          (do (update-entry-fields! coll-name dep-id
                                {:staleness-beta (inc (or (:staleness-beta dep) 1))
                                 :staleness-source "transitive"
                                 :staleness-depth (inc depth)})
                              (inc cnt))
                          cnt)
                        cnt))
                    0
                    kg-outgoing)))))))

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
