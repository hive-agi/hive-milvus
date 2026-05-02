;; PROPRIETARY - Copyright 2026 BuddhiLW. All Rights Reserved.
;; This file is part of hive-milvus and may not be distributed
;; without explicit written permission.

(ns hive-milvus.store.lookup
  "Leaf ns for cross-collection id lookup. Extracted out of
   `hive-milvus.store.entries` so the `relocate/` pipeline can call
   into it without creating a cycle (entries → pipeline → collectors
   → entries). No upward dependencies — only `routing` + `query`."
  (:require [hive-milvus.store.query :as query]
            [hive-milvus.store.routing :as routing]))

(defn legacy-coll-name
  "Pinned config collection (legacy single-coll path). Used as a fan-out
   anchor for type-less reads so users with a custom :collection-name in
   their config still see their entries during the per-dim transition."
  [config-atom]
  (:collection-name @config-atom "hive_mcp_memory"))

(defn known-collections
  "Union of routing/all-known-collections and the legacy-pinned name."
  [config-atom]
  (->> (cons (legacy-coll-name config-atom) (routing/all-known-collections))
       distinct
       vec))

(defn find-entry-collection
  "Locate which known collection holds `id`. Returns the collection
   name or nil. Each lookup is wrapped in try so a missing collection
   (e.g. `hive_mcp_memory_1024d` before the first qwen3-routed write)
   doesn't abort the scan."
  [config-atom id]
  (some (fn [coll]
          (try
            (when (query/get-entry-by-id coll id) coll)
            (catch Exception _ nil)))
        (known-collections config-atom)))
