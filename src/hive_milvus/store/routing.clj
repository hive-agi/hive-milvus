(ns hive-milvus.store.routing
  "Per-type collection routing for Milvus.

   Bridges the injected embedding port's type→provider routing
   (`port/routing-for-type`) to Milvus collection naming (underscores,
   no hyphens). Each provider dimension lives in its own Milvus collection
   so dual-run migrations (e.g. nomic 768-d → qwen3-embedding:0.6b 1024-d)
   coexist without schema conflict — Milvus collections have immutable
   dimension at create time.

   Collection-name conventions:

   - Default 768-d collection: `hive_mcp_memory` (legacy name preserved)
   - Per-dim collections:      `hive_mcp_memory_<dim>d`

   Reads without a type opt fan out across all known collections; writes
   are routed to the entry's type's collection and auto-create on first
   use via `index/ensure-collection!`."
  (:require [hive-milvus.embed.port :as port]
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
          (port/routing-for-type (or memory-type "note"))]
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

(defn- entry-content-str
  "Coerce an entry's :content to a string for size estimation. Mirrors
   `hive-milvus.embedder/entry->content` exactly (JSON for maps) so the
   escalation decision is identical at the collection-routing and embedding
   sites — keeping the stored vector's dimension coherent with the collection."
  [entry]
  (let [raw (or (:content entry) "")]
    (if (map? raw)
      (try ((requiring-resolve 'clojure.data.json/write-str) raw)
           (catch Exception _ (pr-str raw)))
      (str raw))))

(defn coll-for-type+size
  "Like `coll-for-type` but size-aware: when `content` overflows the type's
   routed provider, escalates to the bigger-context provider's collection so
   the stored vector's dimension matches what `embed-for-entry` will produce."
  [memory-type content]
  (try
    (let [{:keys [collection-name dimension max-tokens provider-key]}
          (port/routing-for-type+size (or memory-type "note") content)]
      {:collection-name (chroma->milvus collection-name)
       :dimension       dimension
       :max-tokens      max-tokens
       :provider-key    provider-key})
    (catch Exception e
      (log/debug "coll-for-type+size fallback for" memory-type "—" (.getMessage e))
      {:collection-name base-collection-name
       :dimension       768
       :max-tokens       2048
       :provider-key    :fallback})))

(defn coll-for-entry
  "Resolve the routed Milvus collection for an entry map. Size-aware: oversized
   content escalates to the bigger provider's collection so the stored vector's
   dimension matches. No-embed types (structurally-addressed blobs) route to
   their default-provider collection — a zero placeholder vector of that
   dimension is stored by `embed-for-entry`."
  [entry]
  (if (port/no-embed-type? (:type entry))
    (coll-for-type (:type entry))
    (coll-for-type+size (:type entry) (entry-content-str entry))))

(defn ensure-routed!
  "Ensure the routed Milvus collection exists with the correct dimension
   and is loaded + scalar-indexed. Returns the collection name."
  [memory-type]
  (let [{:keys [collection-name dimension]} (coll-for-type memory-type)]
    (index/ensure-collection! collection-name dimension)
    collection-name))

(defn all-known-collections
  "Milvus collections backing the currently configured embedding providers.
   Derived from `port/collection-names` so reads cover exactly the spaces
   writes can produce."
  []
  (try
    (->> (port/collection-names)
         (map chroma->milvus)
         distinct
         vec)
    (catch Exception _
      [base-collection-name])))
