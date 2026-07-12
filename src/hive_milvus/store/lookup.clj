;; PROPRIETARY - Copyright 2026 BuddhiLW. All Rights Reserved.
;; This file is part of hive-milvus and may not be distributed
;; without explicit written permission.

(ns hive-milvus.store.lookup
  "Leaf ns for cross-collection id lookup. Extracted out of
   `hive-milvus.store.entries` so the `relocate/` pipeline can call
   into it without creating a cycle (entries → pipeline → collectors
   → entries). No upward dependencies — only `routing` + `query`."
  (:require [hive-milvus.store.query :as query]
            [hive-milvus.store.routing :as routing]
            [malli.core :as m]
            [hive-mcp.embeddings.service :as embed-svc]))

(defn legacy-coll-name
  "The collection pinned by `:collection-name` in config, or nil when the
   user has not pinned one."
  [config-atom]
  (:collection-name @config-atom))

(defn- searchable?
  "True when a configured provider can embed a query into `coll`'s space.
   Config-only — the store preloads its collections before the embedding
   registry exists, so this must not build a provider."
  [coll]
  (embed-svc/collection-backed? coll))

(defn known-collections
  "Every collection a type-less read may fan out over: the collections backing
   the configured embedding providers, plus a pinned `:collection-name` when a
   provider still backs it. A collection whose provider is no longer configured
   is not searched."
  [config-atom]
  (->> (cons (legacy-coll-name config-atom) (routing/all-known-collections))
       (remove nil?)
       distinct
       (filterv searchable?)))

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

(m/=> known-collections
      [:=> [:cat [:fn {:error/message "config atom"} #(instance? clojure.lang.IAtom %)]]
       [:vector :string]])