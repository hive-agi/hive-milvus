(ns hive-milvus.store.index
  "Collection loading and scalar-index management for MilvusMemoryStore.

   Owns two process-wide memoization atoms:
   - `loaded-collections`: collections this process has `load-collection`'d.
   - `indexed-collections`: collections we've probed + installed INVERTED
     scalar indexes on.

   Both are `defonce` so hot-reload doesn't desync with milvus-clj's own
   singleton client state."
  (:require [hive-dsl.result :as dsl-r]
            [milvus-clj.api :as milvus]
            [milvus-clj.schema :as milvus-schema]
            [milvus-clj.index :as milvus-index]
            [taoensso.timbre :as log]))

(defonce loaded-collections
  ;; Per-process memo of collections we've already `load-collection`'d.
  ;; `load-collection` is expensive (seconds) while subsequent queries are
  ;; cheap — calling it on every `connect!` regresses cold paths badly.
  ;; Invalidated by `invalidate-loaded-collection!` when reconnect detects
  ;; a fresh server or collection was dropped.
  (atom #{}))

(defn invalidate-loaded-collection!
  [collection-name]
  (swap! loaded-collections disj collection-name))

(def scalar-indexed-fields
  "Fields that back hot catchup filters. Without INVERTED scalar indexes on
   these, Milvus cold-path queries fall back to full collection scans (~50s
   per query on a few-thousand-row collection). Order matters for logging
   only — each index is created independently and idempotently."
  ["type" "project_id"])

(defonce ^{:doc "Memoization guard so we only probe/install scalar indexes once per
   process per collection. defonce so hot-reload doesn't reset the guard
   while loaded-collections (also defonce) still holds the collection —
   that desync caused ensure-scalar-indexes! to be skipped on reconnect."}
  indexed-collections
  (atom #{}))

(defn ensure-scalar-indexes!
  "Idempotently create INVERTED scalar indexes on filter fields used by
   catchup (type, project_id). Milvus `createIndex` on an already-indexed
   field returns a non-OK code; we catch and keep going — the failure mode
   for 'already exists' is indistinguishable from other errors via the
   current milvus-clj surface, so we log at debug + carry on.

   Metric-type is a required param on CreateIndexParam even for scalar
   indexes; Milvus server ignores it when the index type is INVERTED."
  [collection-name]
  (when-not (contains? @indexed-collections collection-name)
    (doseq [field scalar-indexed-fields]
      (let [result (dsl-r/rescue ::index-failed
                     @(milvus/create-index collection-name
                                           {:field-name  field
                                            :index-type  io.milvus.param.IndexType/INVERTED
                                            :metric-type :l2}))]
        (if (= result ::index-failed)
          (log/debug "ensure-scalar-indexes! skipping" field "(likely already indexed)")
          (log/info "Ensured INVERTED scalar index on" field "for" collection-name))))
    (swap! indexed-collections conj collection-name)))

(defn ensure-collection!
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
              (swap! loaded-collections conj collection-name))))))
  (ensure-scalar-indexes! collection-name))
