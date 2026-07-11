(ns hive-milvus.store.lifecycle
  "Connection lifecycle helpers for MilvusMemoryStore."
  (:require [hive-mcp.embeddings.service :as embed-svc]
            [hive-milvus.collections :as collections]
            [hive-milvus.store.health :as health]
            [hive-milvus.store.index :as index]
            [hive-milvus.store.schema :as schema]
            [milvus-clj.api :as milvus]
            [taoensso.timbre :as log]
            [hive-milvus.resilience.reconnect :as reconnect]
            [hive-milvus.resilience.scheduler :as scheduler]))

(defn- dim-from-chroma-name
  "Parse the trailing `-<N>d` suffix from a chroma collection name.
   `hive-mcp-memory-4096d` → 4096; `hive-mcp-memory` → 768 (default)."
  [chroma-name]
  (if-let [m (re-find #"-(\d+)d$" (str chroma-name))]
    (parse-long (second m))
    768))

(defn- preload-known-collections!
  "After the legacy 768-d collection is loaded, walk every chroma-style
   collection name `embed-svc/type->collection-names` knows about and
   ensure each one is created + loaded + scalar-indexed. Without this
   the first write to any non-default-dim collection (e.g. the 4096-d
   collection used by :type/decision) pays the full Milvus
   `load-collection` cost mid-request, easily exceeding the 30 s
   memory-write budget and surfacing as a misleading 'memory add timed
   out' error.

   Each ensure-collection! call is independent + idempotent — a single
   slow load doesn't block the others, and re-running is cheap because
   the per-collection memo short-circuits."
  []
  (try
    (doseq [chroma-name (embed-svc/type->collection-names nil)]
      (let [milvus-name (collections/collection->milvus-name chroma-name)
            dimension   (dim-from-chroma-name chroma-name)]
        (try
          (index/ensure-collection! milvus-name dimension)
          (log/info "Preloaded Milvus collection:" milvus-name "dim:" dimension)
          (catch Throwable t
            (log/warn "Preload of" milvus-name "failed (non-fatal):" (.getMessage t))))))
    (catch Throwable t
      (log/warn "Could not enumerate known collections for preload:" (.getMessage t)))))

(defn connect!
  [config-atom config]
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
        max-ms        15000
        result
        (do
          (swap! config-atom merge config)
          (loop [attempt 1]
            (let [result
                  (try
                    (milvus/connect! milvus-config)
                    (let [dimension (embed-svc/get-dimension-for coll-name)]
                      (index/ensure-collection! coll-name dimension))
                    (preload-known-collections!)
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
                  (recur (inc attempt)))))))]
    ;; Idle heartbeat (proactive resilience): once the connection is
    ;; established — or handed off to the background heal loop — start the
    ;; periodic liveness scheduler so a blip that happens with NO traffic
    ;; still heals, instead of waiting for the next operation to notice.
    ;; Idempotent: a re-connect! won't double-start it.
    (scheduler/start! config-atom)
    result))

(defn disconnect!
  []
  ;; Stop proactive healing BEFORE teardown so neither the idle scheduler nor
  ;; the background reconnect loop races to re-open the client the operator
  ;; just deliberately closed (reconnect/stop! is documented for exactly this
  ;; path; disconnect! previously never called it — a latent race).
  (scheduler/stop!)
  (reconnect/stop!)
  (try
    (milvus/disconnect!)
    (reset! index/loaded-collections #{})
    {:success? true :errors []}
    (catch Exception e
      {:success? false :errors [(.getMessage e)]})))

(defn connected?
  []
  (milvus/connected?))

(defn health-check
  [config-atom]
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
