(ns hive-milvus.store.routing
  "Per-type collection routing for Milvus.

   Bridges hive-mcp's type→provider routing (`embed-svc/resolve-provider-for-type`)
   to Milvus collection naming (underscores, no hyphens). Each provider
   dimension lives in its own Milvus collection so dual-run migrations
   (e.g. nomic 768-d → qwen3-embedding:0.6b 1024-d) coexist without
   schema conflict — Milvus collections have immutable dimension at create
   time.

   Conventions mirror the Chroma path (`hive-mcp.embeddings.service` +
   `chroma.crud`):

   - Default 768-d collection: `hive_mcp_memory` (legacy name preserved)
   - Per-dim collections:      `hive_mcp_memory_<dim>d`

   Reads without a type opt fan out across all known collections; writes
   are routed to the entry's type's collection and auto-create on first
   use via `index/ensure-collection!`."
  (:require [hive-mcp.embeddings.service :as embed-svc]
            [hive-milvus.collections :as collections]
            [hive-milvus.store.index :as index]
            [taoensso.timbre :as log]))

(def ^:private base-collection-name
  "Default 768-d collection name. Matches the legacy single-collection
   default from `store/create-store` so existing data is not orphaned
   by the per-dim refactor."
  "hive_mcp_memory")

(defn- chroma->milvus
  "Convert a Chroma-style collection name (`hive-mcp-memory-1024d`) to
   the Milvus-safe form (`hive_mcp_memory_1024d`). Idempotent for names
   already in Milvus form."
  [chroma-name]
  (collections/collection->milvus-name chroma-name))

(defn coll-for-type
  "Resolve `{:collection-name str :dimension int :max-tokens int :provider-key kw}`
   for a memory type via the global embedder routing. Falls back to the
   legacy default 768-d collection when routing cannot resolve."
  [memory-type]
  (try
    (let [{:keys [collection-name dimension max-tokens provider-key]}
          (embed-svc/resolve-provider-for-type (or memory-type "note"))]
      {:collection-name (chroma->milvus collection-name)
       :dimension       dimension
       :max-tokens      max-tokens
       :provider-key    provider-key})
    (catch Exception e
      (log/debug "coll-for-type fallback for" memory-type "—" (.getMessage e))
      {:collection-name base-collection-name
       :dimension       768
       :max-tokens      2048
       :provider-key    :fallback})))

(defn coll-for-entry
  "Resolve the routed Milvus collection for an entry map. Uses `:type`
   when present, otherwise the default."
  [entry]
  (coll-for-type (:type entry)))

(defn ensure-routed!
  "Ensure the routed Milvus collection exists with the correct dimension
   and is loaded + scalar-indexed. Returns the collection name."
  [memory-type]
  (let [{:keys [collection-name dimension]} (coll-for-type memory-type)]
    (index/ensure-collection! collection-name dimension)
    collection-name))

(defn all-known-collections
  "Return every Milvus collection name we may have written to. Driven by
   the chroma fan-out list in `embed-svc/type->collection-names` so the
   two backends stay in sync."
  []
  (try
    (->> (embed-svc/type->collection-names nil)
         (map chroma->milvus)
         (cons base-collection-name)
         distinct
         vec)
    (catch Exception _
      [base-collection-name])))
