;; PROPRIETARY - Copyright 2026 BuddhiLW. All Rights Reserved.
;; This file is part of hive-milvus and may not be distributed
;; without explicit written permission.

(ns hive-milvus.relocate.promoters.routing
  "PROMOTE: pure functions that compute relocation routing decisions
   from an entry. No I/O, no embedder calls, no Milvus reads — purely
   data → Result.

   Used by `hive-milvus.relocate.pipeline/relocate-one` to decide
   whether an entry needs to move and where it should go before any
   BOUNDARY effect fires."
  (:require [hive-dsl.result :as r]
            [hive-cppb.core :as cppb]
            [hive-milvus.store.routing :as store-routing]))

(cppb/defpromoter compute-target
  "Resolve the canonical target collection for `entry` per current
   type-routing config. Pure data lookup against the in-memory
   embedder config; no remote calls.

   Bundle in:  {:entry e}                 — :entry required
   Bundle out: {:entry e :target-coll s}  — adds :target-coll string

   Returns r/err :routing/no-target when routing can't resolve a
   collection (rare — the fallback collection in store-routing always
   resolves to `hive_mcp_memory`, so this only fires on completely
   broken config)."
  [{:keys [entry] :as bundle}]
  (let [target (-> entry store-routing/coll-for-entry :collection-name)]
    (if (string? target)
      (r/ok (assoc bundle :target-coll target))
      (r/err :routing/no-target
             {:entry-id (:id entry)
              :entry-type (:type entry)
              :resolved target}))))

(cppb/defpromoter classify-relocation-need
  "Compare `:src-coll` (where the entry currently lives) and
   `:target-coll` (where it should live). Tag the bundle with
   `:relocation` ∈ #{:no-op :move} so downstream pipeline stages can
   short-circuit when no move is required.

   Bundle in:  {:src-coll s :target-coll s …}
   Bundle out: same plus {:relocation :no-op | :move}"
  [{:keys [src-coll target-coll] :as bundle}]
  (cond
    (or (nil? src-coll) (nil? target-coll))
    (r/err :routing/missing-coll-info
           {:src-coll src-coll :target-coll target-coll})

    (= src-coll target-coll)
    (r/ok (assoc bundle :relocation :no-op))

    :else
    (r/ok (assoc bundle :relocation :move))))
