(ns hive-milvus.collection.naming
  "L1 pure — collection-name derivation.

   Two pure functions:

   - `chroma-name` : dim → Chroma collection name.
                     768 stays on the legacy `hive-mcp-memory`; other
                     dims get the `-<dim>d` suffix.
   - `milvus-name` : Chroma name → Milvus name (underscores not
                     hyphens). Idempotent — a name already in Milvus
                     form passes through. Reuses
                     `hive-milvus.collections/collection->milvus-name`
                     so the two definitions cannot drift.

   Why this lives outside `routing.clj`: SRP. Routing is about
   resolving a `ProviderSpec`; naming is about projecting a dim to a
   string. The L2 locator composes both."
  (:require [clojure.string :as str]
            [hive-milvus.collections :as collections]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(def ^:const legacy-base-collection
  "Legacy Chroma name preserved for backward compatibility with
   pre-multi-dim data living in 768-d. Once the relocator drains
   this collection, the locator can mark it sunset and reads can
   exclude it."
  "hive-mcp-memory")

(def ^:const legacy-dim
  "Dimension of `legacy-base-collection`. Hard-coded because the
   migration sits at 768 — any other dim already lives in a
   `-<dim>d`-suffixed collection."
  768)

(defn chroma-name
  "Pure — dim → Chroma collection name. 768 → `hive-mcp-memory`.
   Otherwise `hive-mcp-memory-<dim>d`. Throws on non-positive dim
   so a programming error fails at the construction site."
  [dim]
  (when-not (and (integer? dim) (pos? dim))
    (throw (ex-info "Invalid embedding dimension"
                    {:err/tag :collection/invalid-dim
                     :dim     dim})))
  (if (= dim legacy-dim)
    legacy-base-collection
    (str legacy-base-collection "-" dim "d")))

(defn milvus-name
  "Pure — Chroma name → Milvus name (`-` becomes `_`). Idempotent:
   `\"hive_mcp_memory_1024d\"` returns unchanged. Delegates to
   `hive-milvus.collections/collection->milvus-name` so the canonical
   transform lives in one place."
  [chroma-coll-name]
  (collections/collection->milvus-name chroma-coll-name))

(defn dim-of
  "Pure — inverse of `chroma-name`: the dimension the vectors of
   `coll-name` carry, read back from the name. Accepts both the
   Chroma (`-2560d`) and Milvus (`_2560d`) spellings. nil when the
   name is not a memory collection, so a caller can tell 'unknown'
   from 'known and 768'."
  [coll-name]
  (let [n (str/replace (str coll-name) "_" "-")]
    (when (str/starts-with? n legacy-base-collection)
      (if-let [[_ d] (re-find #"-(\d+)d$" n)]
        (parse-long d)
        legacy-dim))))

(defn ref-for-dim
  "Pure — dim → `CollectionRef` map. Computes both Chroma and Milvus
   names. The `:coll/sunset?` flag defaults to false; the locator
   layer flips it on for legacy collections during drain."
  [dim]
  (let [chroma (chroma-name dim)]
    {:coll/name      (milvus-name chroma)
     :coll/dim       dim
     :coll/sunset?   false
     :coll/chroma    chroma}))
