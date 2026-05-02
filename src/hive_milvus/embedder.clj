;; PROPRIETARY - Copyright 2026 BuddhiLW. All Rights Reserved.
;; This file is part of hive-milvus and may not be distributed
;; without explicit written permission.

(ns hive-milvus.embedder
  "BOUNDARY: Re-embed an entry's content via the type-routed embedding
   provider. Extracted out of `hive-milvus.store.schema/entry->record` so
   the schema layer stays a pure record-shape transform — embedding
   dispatch (provider lookup + remote call) lives at the BOUNDARY edge.

   Why this split (CPPB compliance):
     `schema/entry->record` was a PROMOTE that also did COLLECT (provider
     resolution) and BOUNDARY (Ollama embed call). The mix made
     `update-entry-fields!` silently re-embed inside what looked like a
     map-shape conversion — that's the root cause of the Milvus 1804
     dim-mismatch error path. With embedding extracted, callers must
     explicitly invoke `embed-for-entry` first, then build the record
     with the resulting vector. Mismatch becomes visible at the call
     site instead of buried in schema."
  (:require [clojure.string :as str]
            [hive-dsl.result :as r]
            [hive-mcp.embeddings.service :as embed-svc]
            [taoensso.timbre :as log]))

(defn- ^String entry->content
  "Coerce the entry's :content field to a string. Maps are JSON-encoded
   (mirrors the existing `schema/entry->record` behaviour)."
  [entry]
  (let [raw (or (:content entry) "")]
    (if (map? raw)
      (try (clojure.data.json/write-str raw)
           (catch Exception _ (pr-str raw)))
      (str raw))))

(defn- resolve-provider
  "Choose the embedding provider given the entry's :type and the target
   collection name. Type-routed first; falls back to collection-keyed
   for legacy callers without a :type. Returns the provider value or
   nil when neither path resolves."
  [entry collection-name]
  (or (some-> (:type entry)
              embed-svc/resolve-provider-for-type
              :provider)
      (when collection-name
        (try (embed-svc/get-provider-for collection-name)
             (catch Exception _ nil)))))

(defn embed-for-entry
  "Re-embed `entry`'s content using the provider that current routing
   config maps to its :type (or, fallback, to `collection-name`).

   Returns:
     (r/ok embedding-vec)  on success
     (r/err :embedder/no-provider {…})       when neither type nor coll resolves
     (r/err :embedder/embed-failed  {…})     when the embed call throws

   Side-effect: one Ollama / Venice / OpenRouter HTTP call. All other
   work is pure. Caller passes the resulting vector to
   `schema/entry->record`."
  [entry collection-name]
  (let [content (entry->content entry)]
    (cond
      (str/blank? content)
      (r/ok [])

      :else
      (if-let [provider (resolve-provider entry collection-name)]
        (r/try-effect* :embedder/embed-failed
          (let [embed-fn (requiring-resolve 'hive-mcp.chroma.embeddings/embed-text)]
            (embed-fn provider content)))
        (do
          (log/warn "embed-for-entry: no provider resolves for entry"
                    {:type (:type entry) :collection collection-name})
          (r/err :embedder/no-provider
                 {:type            (:type entry)
                  :collection-name collection-name}))))))

(defn embed-for-entry-or-throw
  "Convenience for legacy call sites that historically expected an
   embedding vector (or threw). Prefer `embed-for-entry` in new code so
   errors stay railway-tracked."
  [entry collection-name]
  (let [res (embed-for-entry entry collection-name)]
    (if (r/ok? res)
      (:ok res)
      (throw (ex-info "embed-for-entry failed"
                      {:result res
                       :type   (:type entry)
                       :collection collection-name})))))
