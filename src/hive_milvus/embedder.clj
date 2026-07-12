;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

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
            [hive-mcp.embeddings.resilient :as resilient]
            [hive-milvus.collection.naming :as naming]
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

(defn- resilient-embedder-for
  "Bounded failover embedder for `entry`: the type-routed same-dimension chain,
   or a single collection-keyed provider wrapped as a one-element chain. Returns
   an EmbeddingProvider, or nil when nothing resolves."
  [entry collection-name content]
  (let [chain (some-> (:type entry)
                      (embed-svc/resolve-provider-chain-for-type+size content))]
    (if (seq chain)
      (resilient/resilient-embedder chain)
      (when-let [p (resolve-provider entry collection-name)]
        (resilient/resilient-embedder [{:provider p :provider-key :collection-fallback}])))))

(defn- guard-dim
  "A vector whose width is not the target collection's is not a degraded
   answer, it is a wrong one — Milvus would reject it (1804) or, worse, a
   same-width vector from another model would index as noise. Fail loudly.
   Empty vectors (blank content) pass: the caller decides what to do with them."
  [res collection-name entry]
  (if (and (r/ok? res) (seq (:ok res)))
    (let [expected (naming/dim-of collection-name)
          actual   (count (:ok res))]
      (if (and expected (not= expected actual))
        (do (log/error "embed-for-entry: dimension mismatch"
                       {:collection collection-name :expected expected :actual actual})
            (r/err :embedder/dim-mismatch
                   {:collection collection-name
                    :type       (:type entry)
                    :expected   expected
                    :actual     actual}))
        res))
    res))

(defn- embed-for-entry*
  [entry collection-name]
  (let [content (entry->content entry)]
    (cond
      (str/blank? content)
      (r/ok [])

      ;; Structurally-addressed types (e.g. c4-snapshot) are never semantically
      ;; searched — skip the provider call and emit a zero placeholder vector
      ;; sized to the routed collection's dimension. Milvus requires a fixed-dim
      ;; vector, so an empty vector would be rejected on insert. The dimension
      ;; is taken from the type's DEFAULT provider — matching
      ;; routing/coll-for-entry's no-embed branch (coll-for-type, no escalation).
      (embed-svc/no-embed-type? (:type entry))
      (let [dim (try (:dimension (embed-svc/resolve-provider-for-type (:type entry)))
                     (catch Exception _ 768))]
        (r/ok (vec (repeat (or dim 768) 0.0))))

      :else
      (if-let [embedder (resilient-embedder-for entry collection-name content)]
        (r/try-effect* :embedder/embed-failed
          (let [embed-fn (requiring-resolve 'hive-mcp.chroma.embeddings/embed-text)]
            (embed-fn embedder content)))
        (do
          (log/warn "embed-for-entry: no provider resolves for entry"
                    {:type (:type entry) :collection collection-name})
          (r/err :embedder/no-provider
                 {:type            (:type entry)
                  :collection-name collection-name}))))))

(defn embed-for-entry
  "Re-embed `entry`'s content using the provider that current routing
   config maps to its :type (or, fallback, to `collection-name`).

   Returns:
     (r/ok embedding-vec)  on success
     (r/err :embedder/no-provider {…})       when neither type nor coll resolves
     (r/err :embedder/embed-failed  {…})     when the embed call throws
     (r/err :embedder/dim-mismatch  {…})     when the vector's width is not the
                                             one `collection-name` holds

   Side-effect: one Ollama / Venice / OpenRouter HTTP call. All other
   work is pure. Caller passes the resulting vector to
   `schema/entry->record`."
  [entry collection-name]
  (-> (embed-for-entry* entry collection-name)
      (guard-dim collection-name entry)))

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
