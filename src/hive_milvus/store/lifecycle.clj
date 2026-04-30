(ns hive-milvus.store.lifecycle
  "Connection lifecycle helpers for MilvusMemoryStore."
  (:require [hive-mcp.embeddings.service :as embed-svc]
            [hive-milvus.store.health :as health]
            [hive-milvus.store.index :as index]
            [hive-milvus.store.schema :as schema]
            [milvus-clj.api :as milvus]
            [taoensso.timbre :as log]))

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

(defn disconnect!
  []
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
