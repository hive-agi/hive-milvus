(ns hive-milvus.collection.config
  "Typed config for the collection bounded context, resolved via
   hive-di `defconfig`. Reads `[:milvus :collections]` block from
   `~/.config/hive-mcp/config.edn`.

   Two fields:

   - `:sunset` — set of Milvus collection names that are RETIRED.
                 Locator returns them in `known-collections` (so reads
                 still fan out to legacy data) but excludes from
                 `active-collections` (so writes never land there).
   - `:base-collection-name` — Chroma form of the legacy 768-d
                 collection. Default `\"hive-mcp-memory\"` mirrors
                 `hive-milvus.collection.naming/legacy-base-collection`."
  (:require [hive-di.core :as di]
            [hive-di.source :as src]
            [hive-dsl.result :as r]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(def ^:const config-edn-path
  (str (System/getProperty "user.home") "/.config/hive-mcp/config.edn"))

(di/defconfig CollectionConfig
  :sunset (src/file config-edn-path
                    [:milvus :collections :sunset]
                    :default #{}
                    :type :set
                    :doc "Set of Milvus collection names that are sunset (read-only fan-out, no writes). Background drain flips this on once the relocator finishes copying entries to dim-tagged collections.")

  :base-collection-name (src/file config-edn-path
                                  [:milvus :collections :base-name]
                                  :default "hive-mcp-memory"
                                  :type :string
                                  :doc "Chroma form of the legacy base collection. Mirrors hive-milvus.collection.naming/legacy-base-collection."))

(defn resolve!
  ([] (resolve! {}))
  ([overrides]
   (let [result (resolve-CollectionConfig overrides)]
     (if (r/ok? result)
       (:ok result)
       (throw (ex-info "CollectionConfig resolution failed"
                       {:result result :overrides overrides}))))))

(defn sunset-set
  "Resolved set of sunset collection names."
  []
  (:sunset (resolve!)))

(defn sunset?
  "True iff `coll-name` is in the sunset set."
  [coll-name]
  (contains? (sunset-set) coll-name))
