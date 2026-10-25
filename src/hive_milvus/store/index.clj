(ns hive-milvus.store.index
  "Collection loading and scalar-index management for MilvusMemoryStore.

   Owns two process-wide memoization atoms:
   - `loaded-collections`: collections this process has `load-collection`'d.
   - `indexed-collections`: collections we've probed + installed INVERTED
     scalar indexes on.

   Both are `defonce` so hot-reload doesn't desync with milvus-clj's own
   singleton client state."
  (:require [hive-milvus.store.deref :as d]
            [milvus-clj.api :as milvus]
            [milvus-clj.schema :as milvus-schema]
            [milvus-clj.index :as milvus-index]
            [taoensso.timbre :as log]
            [hive-milvus.store.rest-index :as rest-index]
            [milvus-clj.config :as milvus-config]))

(defonce loaded-collections
  ;; Per-process memo of collections we've already `load-collection`'d.
  ;; `load-collection` is expensive (seconds) while subsequent queries are
  ;; cheap — calling it on every `connect!` regresses cold paths badly.
  ;; Invalidated by `invalidate-loaded-collection!` when reconnect detects
  ;; a fresh server or collection was dropped.
  (atom #{}))

(declare indexed-collections)

(defn invalidate-loaded-collection!
  [collection-name]
  (swap! loaded-collections disj collection-name)
  (swap! indexed-collections disj collection-name))

(def scalar-indexed-fields
  "Equality/IN fields used by catchup. Tags are serialized VarChar with
   infix LIKE filters; these indexes do not accelerate that predicate."
  ["type" "project_id"])

(defonce ^{:doc "Memoization guard so we only probe/install scalar indexes once per
   process per collection. defonce so hot-reload doesn't reset the guard
   while loaded-collections (also defonce) still holds the collection —
   that desync caused ensure-scalar-indexes! to be skipped on reconnect."}
  indexed-collections
  (atom #{}))

(defn ensure-indexed!
  "Serialize installation per memo. A false result means build pending.
   Partial passes, pending builds and failures are never memoized."
  [memo collection-name install!]
  (locking memo
    (when-not (contains? @memo collection-name)
      (try
        (if (false? (install!))
          false
          (do (swap! memo conj collection-name)
              (log/info "Scalar indexes ensured for" collection-name)
              true))
        (catch Exception e
          (log/warn e "Scalar indexes not ensured; will retry" collection-name)
          false)))))

(defn ensure-scalar-indexes!
  "Ensure type/project_id indexes. HTTP probes actual field definitions.
   HTTP memo is populated only after both builds report Finished."
  [collection-name]
  (ensure-indexed! indexed-collections collection-name
    (fn []
      (let [config (milvus-config/get-config)]
        (if (= :http (:transport config))
          (rest-index/ensure-fields! (partial rest-index/post! config)
                                     collection-name scalar-indexed-fields)
          (doseq [field scalar-indexed-fields]
            (d/deref! :create-index
                      (milvus/create-index collection-name
                        {:field-name field
                         :index-type io.milvus.param.IndexType/INVERTED
                         :metric-type :l2}))))))))

(defn ensure-collection!
  "Ensure the memory collection exists and is loaded into memory.
   After Milvus restart, collections exist but aren't loaded — queries
   return empty unless we explicitly load. The per-process memo
   (`loaded-collections`) is consulted FIRST: a memo hit issues no RPC at
   all, so a write path calling this per entry costs nothing after the
   first call. Only a miss probes has-collection, then creates or loads.
   `invalidate-loaded-collection!` drops the memo when reconnect detects
   a fresh server or a dropped collection."
  [collection-name dimension]
  (when-not (contains? @loaded-collections collection-name)
    (if-not (d/deref! :has-collection (milvus/has-collection collection-name))
      (do (log/info "Creating Milvus collection:" collection-name "dim:" dimension)
          (d/deref! :create-collection (milvus/create-collection collection-name
             {:schema (milvus-schema/with-dimension dimension)
              :index  milvus-index/default-memory-index
              :description "hive-mcp memory store"}))
          (swap! loaded-collections conj collection-name))
      (do (log/info "Loading Milvus collection into memory:" collection-name)
          (try
            (d/deref! :load-collection (milvus/load-collection collection-name))
            (swap! loaded-collections conj collection-name)
            (catch Exception e
              (log/debug "load-collection (may already be loaded):" (.getMessage e))
              (swap! loaded-collections conj collection-name))))))
  (ensure-scalar-indexes! collection-name))
