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
            [hive-milvus.store.schema :as schema]
            [hive-milvus.store.health :as health :refer [resilient]]
            [taoensso.timbre :as log]
            [hive-milvus.store.lifecycle :as lifecycle]
            [hive-milvus.store.entries :as entries]
            [hive-milvus.store.analytics :as analytics]
            [hive-milvus.store.staleness :as staleness]
            [hive-milvus.store.batch :as batch]))

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
;; Ordering — Milvus query-scalar lacks server-side ORDER BY. We sort
;; post-fetch on the rows the server returned, so callers MUST pass a
;; :limit large enough to cover the desired top-N (otherwise the sort
;; is over an arbitrary scan-order subset).
;; =========================================================================

;; =========================================================================
;; Protocol Implementation
;; =========================================================================

(defrecord MilvusMemoryStore [config-atom]
  proto/IMemoryStore

  (connect! [_this config]
    (lifecycle/connect! config-atom config))

  (disconnect! [_this]
    (lifecycle/disconnect!))

  (connected? [_this]
    (lifecycle/connected?))

  (health-check [_this]
    (lifecycle/health-check config-atom))

  (add-entry! [_this entry]
    (entries/add-entry! config-atom entry))

  (get-entry [_this id]
    (entries/get-entry config-atom id))

  (update-entry! [_this id updates]
    (entries/update-entry! config-atom id updates))

  (delete-entry! [_this id]
    (entries/delete-entry! config-atom id))

  (query-entries [_this opts]
    (entries/query-entries config-atom opts))

  (search-similar [_this query-text opts]
    (entries/search-similar config-atom query-text opts))

  (supports-semantic-search? [_this]
    (entries/supports-semantic-search? config-atom))

  (cleanup-expired! [_this]
    (entries/cleanup-expired! config-atom))

  (entries-expiring-soon [_this days opts]
    (entries/entries-expiring-soon config-atom days opts))

  (find-duplicate [_this type content-hash opts]
    (entries/find-duplicate config-atom type content-hash opts))

  (store-status [_this]
    (entries/store-status config-atom))

  (reset-store! [_this]
    (entries/reset-store! config-atom))

  proto/IMemoryStoreWithAnalytics

  (log-access! [_this id]
    (analytics/log-access! config-atom id))

  (record-feedback! [_this id feedback]
    (analytics/record-feedback! config-atom id feedback))

  (get-helpfulness-ratio [_this id]
    (analytics/get-helpfulness-ratio config-atom id))

  proto/IMemoryStoreWithStaleness

  (update-staleness! [_this id staleness-opts]
    (staleness/update-staleness! config-atom id staleness-opts))

  (get-stale-entries [_this threshold opts]
    (staleness/get-stale-entries config-atom threshold opts))

  (propagate-staleness! [_this source-id depth]
    (staleness/propagate-staleness! config-atom source-id depth))

  proto/IMemoryStoreBatch

  (get-entries [_this ids]
    (batch/get-entries config-atom ids))

  proto/IMemoryStoreWithRouting

  (target-collection-for [_this entry]
    (entries/target-collection-for config-atom entry))

  (relocate-entry! [_this id]
    (entries/relocate-entry! config-atom id)))

(extend-protocol proto/IMemoryStoreMetadataWrite
  MilvusMemoryStore
  (update-metadata! [this id updates]
    (entries/update-fields-keep-embedding! (:config-atom this) id updates)))

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